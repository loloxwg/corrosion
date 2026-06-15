//! gossip 节点：UDP 收发 + 两条传播路 + 成员/故障检测。
//!
//! 推送路：写入进 recent 缓冲；每 push_interval 随机挑 K 个存活 peer 发 Push。
//!         收到新变更会转存进缓冲继续转发，形成多跳病毒式扩散。
//! 对账路：每 sync_interval 随机挑 1 个 peer，交换摘要补齐缺/旧条目，
//!         兜底 UDP 丢包并让新节点追平。
//! 故障检测：每收到 peer 任意报文刷新 last_seen；超时依次转 suspect / dead。

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crdts::{CmRDT, lwwreg::LWWReg, orswot::Orswot, pncounter::PNCounter};
use rand::seq::IteratorRandom;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::hlc::Hlc;
use crate::store::{Crdt, Mutation, RegVal, Store};
use crate::wire::{Msg, NodeId};

const RECENT_CAP: usize = 256; // recent 缓冲上限
const PUSH_FANOUT: usize = 3; // 每轮推送的目标数
const MAX_FRAME: usize = 4 * 1024 * 1024; // TCP 帧上限，防恶意超大长度前缀
const MAX_SYNC_CONNS: usize = 32; // 并发对账连接上限，防连接洪泛 OOM
const SYNC_CONN_TIMEOUT: Duration = Duration::from_secs(5); // 单连接读写超时，防 slowloris

/// TCP 流上写一个长度前缀帧（4 字节大端长度 + 内容）。
async fn write_frame(stream: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    stream.write_all(bytes).await?;
    Ok(())
}

/// TCP 流上读一个长度前缀帧。
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    Alive,
    Suspect,
    Dead,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub addr: SocketAddr,
    pub last_seen: Instant,
    pub state: PeerState,
    /// 进入 suspect 的时刻，用于判定何时转 dead。
    pub suspect_since: Option<Instant>,
}

const INDIRECT_K: usize = 3; // 间接探测的帮手数

/// 各类周期与超时阈值，集中管理便于测试调小。
#[derive(Debug, Clone, Copy)]
pub struct Timings {
    pub push_interval: Duration,
    pub sync_interval: Duration,
    pub probe_period: Duration,
    pub ack_timeout: Duration,
    pub indirect_timeout: Duration,
    pub failure_tick: Duration,
    pub suspect_to_dead: Duration,
}

impl Default for Timings {
    fn default() -> Self {
        Timings {
            push_interval: Duration::from_millis(500),
            sync_interval: Duration::from_secs(2),
            probe_period: Duration::from_secs(1),
            ack_timeout: Duration::from_millis(300),
            indirect_timeout: Duration::from_millis(400),
            failure_tick: Duration::from_millis(500),
            suspect_to_dead: Duration::from_secs(3),
        }
    }
}

pub struct Node {
    pub id: NodeId,
    pub addr: SocketAddr,
    socket: Arc<UdpSocket>,
    tcp: Arc<TcpListener>,
    store: Arc<dyn Store>,
    hlc: Arc<Hlc>,
    peers: Mutex<HashMap<NodeId, PeerInfo>>,
    recent: Mutex<VecDeque<Mutation>>,
    timings: Timings,
    // SWIM 探测状态
    seq: std::sync::atomic::AtomicU64,
    acked: Mutex<std::collections::HashSet<u64>>, // 已收到 ack 的 seq
    relays: Mutex<HashMap<u64, (SocketAddr, u64)>>, // helper_seq -> (origin_addr, origin_seq)
    probe_queue: Mutex<VecDeque<NodeId>>,         // round-robin 探测队列
}

impl Node {
    pub async fn bind(
        id: NodeId,
        addr: SocketAddr,
        store: Arc<dyn Store>,
        seeds: Vec<SocketAddr>,
        timings: Timings,
    ) -> anyhow::Result<Arc<Self>> {
        let socket = Arc::new(UdpSocket::bind(addr).await?);
        let local = socket.local_addr()?;
        // TCP 监听同一地址（UDP/TCP 端口号可共用），用于对账与大消息。
        let tcp = Arc::new(TcpListener::bind(local).await?);
        let mut peers = HashMap::new();
        // 种子节点先以 id=0 占位记下地址；真正的 id 在收到它们的报文后修正。
        for (i, seed) in seeds.into_iter().enumerate() {
            // 用一个临时负 id 空间避免和真实 id 冲突：这里用 u16 高位段。
            let placeholder = u16::MAX - i as u16;
            peers.insert(
                placeholder,
                PeerInfo {
                    addr: seed,
                    last_seen: Instant::now(),
                    state: PeerState::Alive,
                    suspect_since: None,
                },
            );
        }
        Ok(Arc::new(Node {
            id,
            addr: local,
            socket,
            tcp,
            store,
            hlc: Arc::new(Hlc::new()),
            peers: Mutex::new(peers),
            recent: Mutex::new(VecDeque::new()),
            timings,
            seq: std::sync::atomic::AtomicU64::new(0),
            acked: Mutex::new(std::collections::HashSet::new()),
            relays: Mutex::new(HashMap::new()),
            probe_queue: Mutex::new(VecDeque::new()),
        }))
    }

    // ---- 对外 API（被 HTTP 层调用） ----

    /// 落库 + 入 recent 缓冲，统一出口。
    fn commit(&self, key: String, crdt: Crdt) {
        let m = Mutation { key, crdt };
        self.store.apply(&m);
        self.buffer(m);
    }

    // ---- Register（覆盖型 KV，LWW） ----

    /// 写入：覆盖型，marker=(hlc,node)，并发取 marker 大者。
    pub fn put(&self, key: String, value: Vec<u8>) {
        let marker = (self.hlc.tick(), self.id);
        let reg = LWWReg {
            val: RegVal::Val(value),
            marker,
        };
        self.commit(key, Crdt::Register(reg));
    }

    /// 删除：写一个 Deleted 墓碑，靠 marker 与并发写竞争。
    pub fn delete(&self, key: String) {
        let marker = (self.hlc.tick(), self.id);
        let reg = LWWReg {
            val: RegVal::Deleted,
            marker,
        };
        self.commit(key, Crdt::Register(reg));
    }

    /// 读取寄存器值。墓碑或非寄存器类型返回 None。
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        match self.store.get(key)? {
            Crdt::Register(r) => match r.val {
                RegVal::Val(b) => Some(b),
                RegVal::Deleted => None,
            },
            _ => None,
        }
    }

    // ---- Counter（PN-Counter，增量不丢） ----

    /// 计数器增量（delta 可负）。并发增量自动求和。
    pub fn counter_add(&self, key: String, delta: i64) {
        let mut c = match self.store.get(&key) {
            Some(Crdt::Counter(c)) => c,
            _ => PNCounter::new(),
        };
        let op = if delta >= 0 {
            c.inc_many(self.id, delta as u64)
        } else {
            c.dec_many(self.id, (-delta) as u64)
        };
        c.apply(op);
        self.commit(key, Crdt::Counter(c));
    }

    /// 读取计数器当前值；非计数器类型返回 None。
    pub fn counter_read(&self, key: &str) -> Option<i64> {
        match self.store.get(key)? {
            Crdt::Counter(c) => Some(c.read().to_string().parse().unwrap_or(0)),
            _ => None,
        }
    }

    // ---- Set（OR-Set，并发求并） ----

    pub fn set_add(&self, key: String, elem: String) {
        let mut s = match self.store.get(&key) {
            Some(Crdt::Set(s)) => s,
            _ => Orswot::new(),
        };
        let op = s.add(elem, s.read_ctx().derive_add_ctx(self.id));
        s.apply(op);
        self.commit(key, Crdt::Set(s));
    }

    pub fn set_remove(&self, key: String, elem: String) {
        let mut s = match self.store.get(&key) {
            Some(Crdt::Set(s)) => s,
            _ => return,
        };
        let op = s.rm(elem.clone(), s.contains(&elem).derive_rm_ctx());
        s.apply(op);
        self.commit(key, Crdt::Set(s));
    }

    /// 读取集合成员；非集合类型返回 None。
    pub fn set_read(&self, key: &str) -> Option<Vec<String>> {
        match self.store.get(key)? {
            Crdt::Set(s) => {
                let mut v: Vec<String> = s.read().val.into_iter().collect();
                v.sort();
                Some(v)
            }
            _ => None,
        }
    }

    /// 把一个 CRDT 渲染成可读的 (类型, 值) 供调试展示。
    fn render(crdt: &Crdt) -> (&'static str, serde_json::Value) {
        let v = match crdt {
            Crdt::Register(r) => match &r.val {
                RegVal::Val(b) => serde_json::json!(String::from_utf8_lossy(b)),
                RegVal::Deleted => serde_json::json!("<deleted>"),
            },
            Crdt::Counter(c) => serde_json::json!(c.read().to_string()),
            Crdt::Set(s) => {
                let mut m: Vec<String> = s.read().val.into_iter().collect();
                m.sort();
                serde_json::json!(m)
            }
        };
        (crdt.type_name(), v)
    }

    /// 内部状态全量 dump，供 /debug 观察 gossip 运行细节。
    pub fn debug_dump(&self) -> serde_json::Value {
        let data: Vec<_> = self
            .store
            .snapshot()
            .into_iter()
            .map(|m| {
                let (ty, val) = Self::render(&m.crdt);
                serde_json::json!({ "key": m.key, "type": ty, "value": val })
            })
            .collect();
        let recent: Vec<_> = self
            .recent
            .lock()
            .unwrap()
            .iter()
            .map(|m| {
                let (ty, val) = Self::render(&m.crdt);
                serde_json::json!({ "key": m.key, "type": ty, "value": val })
            })
            .collect();
        let peers: Vec<_> = self
            .peers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, info)| {
                serde_json::json!({
                    "id": id,
                    "addr": info.addr.to_string(),
                    "state": format!("{:?}", info.state),
                    "idle_ms": info.last_seen.elapsed().as_millis() as u64,
                })
            })
            .collect();
        serde_json::json!({
            "id": self.id,
            "addr": self.addr.to_string(),
            "data": data,
            "recent_buffer": recent,
            "peers": peers,
        })
    }

    /// 当前成员视图（含自己），供 /members 展示。
    pub fn members(&self) -> Vec<(NodeId, String, String)> {
        let mut out = vec![(self.id, self.addr.to_string(), "self".to_string())];
        for (id, info) in self.peers.lock().unwrap().iter() {
            let state = match info.state {
                PeerState::Alive => "alive",
                PeerState::Suspect => "suspect",
                PeerState::Dead => "dead",
            };
            out.push((*id, info.addr.to_string(), state.to_string()));
        }
        out
    }

    // ---- 内部辅助 ----

    fn buffer(&self, m: Mutation) {
        let mut recent = self.recent.lock().unwrap();
        recent.push_back(m);
        while recent.len() > RECENT_CAP {
            recent.pop_front();
        }
    }

    /// 记录/刷新一个 peer 的存活。忽略自己。
    fn note_peer(&self, id: NodeId, addr: SocketAddr) {
        if id == self.id {
            return;
        }
        let mut peers = self.peers.lock().unwrap();
        // 若该地址原先以占位 id 登记过，先清掉占位项，避免重复。
        peers.retain(|pid, info| !(info.addr == addr && *pid != id));
        peers
            .entry(id)
            .and_modify(|p| {
                p.addr = addr;
                p.last_seen = Instant::now();
                p.state = PeerState::Alive;
                p.suspect_since = None;
            })
            .or_insert(PeerInfo {
                addr,
                last_seen: Instant::now(),
                state: PeerState::Alive,
                suspect_since: None,
            });
    }

    /// 探测成功：peer 明确活着，回 alive。
    fn mark_alive(&self, id: NodeId) {
        if let Some(p) = self.peers.lock().unwrap().get_mut(&id) {
            p.last_seen = Instant::now();
            p.state = PeerState::Alive;
            p.suspect_since = None;
        }
    }

    /// 探测全失败：从 alive 转 suspect（记下时刻，供后续转 dead）。
    fn mark_suspect(&self, id: NodeId) {
        if let Some(p) = self.peers.lock().unwrap().get_mut(&id) {
            if p.state == PeerState::Alive {
                p.state = PeerState::Suspect;
                p.suspect_since = Some(Instant::now());
            }
        }
    }

    /// 挑选若干「非 dead」peer 的地址用于推送。
    fn pick_peers(&self, k: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        self.peers
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.state != PeerState::Dead)
            .map(|p| p.addr)
            .sample(&mut rng, k)
    }

    fn all_peer_pairs(&self) -> Vec<(NodeId, String)> {
        self.peers
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, _)| **id != u16::MAX && **id < u16::MAX - 16) // 跳过占位 id
            .map(|(id, info)| (*id, info.addr.to_string()))
            .collect()
    }

    async fn send(&self, to: SocketAddr, msg: &Msg) {
        let bytes = msg.encode();
        let _ = self.socket.send_to(&bytes, to).await;
    }

    // ---- 报文处理 ----

    /// 处理一条 UDP 报文。UDP 只跑推送路；对账走 TCP。
    async fn handle(&self, msg: Msg, src: SocketAddr) {
        let (from, _addr) = msg.sender();
        // 用实际来源地址登记发送者，比报文里自报的更可靠。
        self.note_peer(from, src);

        match msg {
            Msg::Push {
                mutations, peers, ..
            } => {
                // 学习对方携带的 peer 列表（传递式发现）。
                for (pid, paddr) in peers {
                    if let Ok(sa) = paddr.parse::<SocketAddr>() {
                        self.note_peer(pid, sa);
                    }
                }
                self.absorb(mutations);
            }
            // SWIM 直接探测：立即回 ack。
            Msg::Ping { seq, .. } => {
                let ack = Msg::Ack {
                    from: self.id,
                    addr: self.addr.to_string(),
                    seq,
                };
                self.send(src, &ack).await;
            }
            // SWIM 应答：若是别人代答的中继 ack，转回给原发起者；否则记下本机 seq 已 ack。
            Msg::Ack { seq, .. } => {
                let relay = self.relays.lock().unwrap().remove(&seq);
                if let Some((origin_addr, origin_seq)) = relay {
                    let fwd = Msg::Ack {
                        from: self.id,
                        addr: self.addr.to_string(),
                        seq: origin_seq,
                    };
                    self.send(origin_addr, &fwd).await;
                } else {
                    self.acked.lock().unwrap().insert(seq);
                }
            }
            // SWIM 间接探测：作为帮手代 ping target，得到 ack 后回中继给发起者。
            Msg::PingReq {
                seq, target, from, ..
            } => {
                if let Ok(target_addr) = target.parse::<SocketAddr>() {
                    let helper_seq = self.next_seq();
                    let origin_addr = src;
                    self.relays
                        .lock()
                        .unwrap()
                        .insert(helper_seq, (origin_addr, seq));
                    let _ = from; // 发起者身份由 src 决定
                    let ping = Msg::Ping {
                        from: self.id,
                        addr: self.addr.to_string(),
                        seq: helper_seq,
                    };
                    self.send(target_addr, &ping).await;
                }
            }
            // 对账消息不应走 UDP，忽略。
            Msg::SyncReq { .. } | Msg::SyncResp { .. } => {}
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// 计算要回给对账请求方的条目：对方缺失或内容哈希不同的（CRDT merge 幂等，多发无害）。
    fn compute_sync_response(&self, digest: &HashMap<String, u64>) -> Vec<Mutation> {
        let mut to_send = Vec::new();
        for m in self.store.snapshot() {
            match digest.get(&m.key) {
                None => to_send.push(m),
                Some(&their_hash) if m.crdt.content_hash() != their_hash => to_send.push(m),
                _ => {}
            }
        }
        to_send
    }

    /// 吸收一批远端变更：刷新 HLC（取寄存器 marker）、落库，新接受的转存继续转发。
    fn absorb(&self, mutations: Vec<Mutation>) {
        for m in mutations {
            if let Crdt::Register(r) = &m.crdt {
                self.hlc.observe(r.marker.0);
            }
            if self.store.apply(&m) {
                self.buffer(m); // 多跳转发
            }
        }
    }

    // ---- 后台循环 ----

    /// 启动收发与周期任务，返回后台运行。
    pub fn spawn(self: &Arc<Self>) {
        self.clone().spawn_recv();
        self.clone().spawn_tcp_server();
        self.clone().spawn_push();
        self.clone().spawn_sync();
        self.clone().spawn_probe();
        self.clone().spawn_failure();
    }

    fn spawn_recv(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match self.socket.recv_from(&mut buf).await {
                    Ok((n, src)) => {
                        if let Some(msg) = Msg::decode(&buf[..n]) {
                            self.handle(msg, src).await;
                        }
                    }
                    Err(_) => continue,
                }
            }
        });
    }

    fn spawn_push(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.timings.push_interval);
            loop {
                ticker.tick().await;
                let mutations: Vec<Mutation> =
                    self.recent.lock().unwrap().iter().cloned().collect();
                if mutations.is_empty() {
                    continue;
                }
                let msg = Msg::Push {
                    from: self.id,
                    addr: self.addr.to_string(),
                    mutations,
                    peers: self.all_peer_pairs(),
                };
                for target in self.pick_peers(PUSH_FANOUT) {
                    self.send(target, &msg).await;
                }
            }
        });
    }

    /// 对账服务端：接受 TCP 连接，读 SyncReq，回 SyncResp。
    /// 用信号量限并发连接数 + 每连接超时，防连接洪泛与 slowloris。
    fn spawn_tcp_server(self: Arc<Self>) {
        let limit = Arc::new(tokio::sync::Semaphore::new(MAX_SYNC_CONNS));
        tokio::spawn(async move {
            loop {
                match self.tcp.accept().await {
                    Ok((mut stream, src)) => {
                        // 拿不到名额就直接丢这条连接，不无限堆积。
                        let Ok(permit) = limit.clone().try_acquire_owned() else {
                            continue;
                        };
                        let node = self.clone();
                        tokio::spawn(async move {
                            let _ = tokio::time::timeout(
                                SYNC_CONN_TIMEOUT,
                                node.serve_sync(&mut stream, src),
                            )
                            .await;
                            drop(permit);
                        });
                    }
                    Err(_) => continue,
                }
            }
        });
    }

    async fn serve_sync(&self, stream: &mut TcpStream, src: SocketAddr) -> std::io::Result<()> {
        let bytes = read_frame(stream).await?;
        if let Some(Msg::SyncReq { from, addr, digest }) = Msg::decode(&bytes) {
            // 信任退化缓解：IP 用连接真实来源，仅端口取自报文（TCP 来源端口是临时端口）。
            // 完整 auth（TLS/签名握手）仍待补，见 spec §8.1。
            if let Ok(reported) = addr.parse::<SocketAddr>() {
                let trusted = SocketAddr::new(src.ip(), reported.port());
                self.note_peer(from, trusted);
            }
            let mutations = self.compute_sync_response(&digest);
            let resp = Msg::SyncResp {
                from: self.id,
                addr: self.addr.to_string(),
                mutations,
            };
            write_frame(stream, &resp.encode()).await?;
        }
        Ok(())
    }

    /// 对账客户端：周期随机挑 1 个 peer，TCP 拉取自己缺/旧的条目。
    fn spawn_sync(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.timings.sync_interval);
            loop {
                ticker.tick().await;
                if let Some(&target) = self.pick_peers(1).first() {
                    let node = self.clone();
                    tokio::spawn(async move {
                        let _ =
                            tokio::time::timeout(SYNC_CONN_TIMEOUT, node.sync_with(target)).await;
                    });
                }
            }
        });
    }

    async fn sync_with(&self, target: SocketAddr) -> std::io::Result<()> {
        let mut stream = TcpStream::connect(target).await?;
        let req = Msg::SyncReq {
            from: self.id,
            addr: self.addr.to_string(),
            digest: self.store.digest(),
        };
        write_frame(&mut stream, &req.encode()).await?;
        let bytes = read_frame(&mut stream).await?;
        if let Some(Msg::SyncResp { mutations, .. }) = Msg::decode(&bytes) {
            self.absorb(mutations);
        }
        Ok(())
    }

    /// SWIM 探测循环：每周期 round-robin 取一个 peer，直接 ping，失败转间接，再失败标 suspect。
    fn spawn_probe(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.timings.probe_period);
            loop {
                ticker.tick().await;
                if let Some(target) = self.next_probe_target() {
                    let node = self.clone();
                    tokio::spawn(async move { node.probe(target).await });
                }
            }
        });
    }

    /// 探测单个 peer：直接 ping → 等 ack → 失败则间接 ping-req → 仍失败标 suspect。
    async fn probe(&self, target: NodeId) {
        let Some(target_addr) = self.peer_addr(target) else {
            return;
        };
        let seq = self.next_seq();
        self.send(
            target_addr,
            &Msg::Ping {
                from: self.id,
                addr: self.addr.to_string(),
                seq,
            },
        )
        .await;
        tokio::time::sleep(self.timings.ack_timeout).await;

        if !self.has_ack(seq) {
            // 直接探测失败 → 找 K 个帮手代为探测。
            let helpers = self.pick_helpers(target, INDIRECT_K);
            for helper in helpers {
                self.send(
                    helper,
                    &Msg::PingReq {
                        from: self.id,
                        addr: self.addr.to_string(),
                        seq,
                        target: target_addr.to_string(),
                    },
                )
                .await;
            }
            tokio::time::sleep(self.timings.indirect_timeout).await;
        }

        if self.take_ack(seq) {
            self.mark_alive(target);
        } else {
            self.mark_suspect(target);
        }
    }

    /// 只查某 seq 是否已 ack（不清除），供间接探测决策。
    fn has_ack(&self, seq: u64) -> bool {
        self.acked.lock().unwrap().contains(&seq)
    }

    /// 查并清除某 seq 的 ack 标记，返回是否已 ack（探测结束时调用一次）。
    fn take_ack(&self, seq: u64) -> bool {
        self.acked.lock().unwrap().remove(&seq)
    }

    /// round-robin 选下一个探测目标（队空则用当前非 dead peer 洗牌重填）。
    fn next_probe_target(&self) -> Option<NodeId> {
        let mut q = self.probe_queue.lock().unwrap();
        if q.is_empty() {
            use rand::seq::SliceRandom;
            let mut ids: Vec<NodeId> = self
                .peers
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, p)| p.state != PeerState::Dead)
                .map(|(id, _)| *id)
                .collect();
            ids.shuffle(&mut rand::rng());
            q.extend(ids);
        }
        q.pop_front()
    }

    fn peer_addr(&self, id: NodeId) -> Option<SocketAddr> {
        self.peers.lock().unwrap().get(&id).map(|p| p.addr)
    }

    /// 选 K 个帮手地址（非 dead、排除 target 自己）。
    fn pick_helpers(&self, target: NodeId, k: usize) -> Vec<SocketAddr> {
        self.peers
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, p)| **id != target && p.state != PeerState::Dead)
            .map(|(_, p)| p.addr)
            .sample(&mut rand::rng(), k)
    }

    /// 故障 tick：把 suspect 超时的 peer 转 dead。
    fn spawn_failure(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.timings.failure_tick);
            loop {
                ticker.tick().await;
                let now = Instant::now();
                let mut peers = self.peers.lock().unwrap();
                for info in peers.values_mut() {
                    if info.state == PeerState::Suspect
                        && let Some(since) = info.suspect_since
                        && now.duration_since(since) > self.timings.suspect_to_dead
                    {
                        info.state = PeerState::Dead;
                    }
                }
            }
        });
    }
}
