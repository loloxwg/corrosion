use std::collections::HashMap;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use crate::actor::MemberId;
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};
use serde_with::{formats::PreferOne, serde_as, OneOrMany};

pub const DEFAULT_GOSSIP_PORT: u16 = 4001;
const DEFAULT_GOSSIP_IDLE_TIMEOUT: u32 = 30;

#[cfg(test)]
pub const DEFAULT_MAX_SYNC_BACKOFF: u32 = 2;
#[cfg(not(test))]
pub const DEFAULT_MAX_SYNC_BACKOFF: u32 = 15;

const fn default_apply_batch_min() -> usize {
    100
}

const fn default_apply_batch_step() -> usize {
    500
}

const fn default_apply_batch_max() -> usize {
    16_000
}

const fn default_batch_threshold_ratio() -> f64 {
    0.9
}

const fn default_cache_size_kib() -> i64 {
    -1048576 // 1 GB (negative value means KiB)
}

const fn default_reaper_interval() -> usize {
    3600
}

const fn default_reaper_limit() -> usize {
    500
}

const fn default_wal_threshold() -> usize {
    // Default of 5GB
    5 * 1024
}

const fn default_processing_queue() -> usize {
    20000
}

/// Used for the apply channel
const fn default_huge_channel() -> usize {
    2048
}

//
const fn default_big_channel() -> usize {
    1024
}

const fn default_mid_channel() -> usize {
    512
}

const fn default_small_channel() -> usize {
    256
}

const fn default_apply_timeout() -> usize {
    10
}

fn default_sql_tx_timeout() -> usize {
    60
}

fn default_min_sync_backoff() -> u32 {
    1
}

fn default_max_sync_backoff() -> u32 {
    DEFAULT_MAX_SYNC_BACKOFF
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub db: DbConfig,
    pub api: ApiConfig,
    pub gossip: GossipConfig,

    #[serde(default)]
    pub perf: PerfConfig,

    #[serde(default)]
    pub admin: AdminConfig,

    #[serde(default)]
    pub telemetry: TelemetryConfig,

    #[serde(default)]
    pub log: LogConfig,
    #[serde(default)]
    pub consul: Option<ConsulConfig>,
    #[serde(default)]
    pub reaper: Option<ReaperConfig>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct TelemetryConfig {
    pub prometheus: Option<PrometheusConfig>,
    pub open_telemetry: Option<OtelConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OtelConfig {
    FromEnv,
    Exporter { endpoint: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminConfig {
    #[serde(alias = "path")]
    pub uds_path: Utf8PathBuf,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            uds_path: default_admin_path(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbConfig {
    pub path: Utf8PathBuf,
    #[serde(default)]
    pub schema_paths: Vec<Utf8PathBuf>,
    #[serde(default)]
    pub subscriptions_path: Option<Utf8PathBuf>,
    /// SQLite page cache size in KiB for writes (negative value).
    /// Default: -1048576 (1 GB). Larger values improve write performance but use more RAM.
    /// WARNING: Setting this too low (<100MB) can severely degrade performance.
    #[serde(default = "default_cache_size_kib")]
    pub cache_size_kib: i64,
}

impl DbConfig {
    pub fn subscriptions_path(&self) -> Utf8PathBuf {
        self.subscriptions_path
            .as_ref()
            .cloned()
            .unwrap_or_else(|| {
                self.path
                    .parent()
                    .map(|parent| parent.join("subscriptions"))
                    .unwrap_or_else(|| "/subscriptions".into())
            })
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(alias = "addr")]
    #[serde_as(deserialize_as = "OneOrMany<_, PreferOne>")]
    pub bind_addr: Vec<SocketAddr>,
    #[serde(default)]
    pub endpoint_name: Option<String>,
    #[serde(alias = "authz", default)]
    pub authorization: Option<AuthzConfig>,
    #[serde_as(deserialize_as = "Option<OneOrMany<_, PreferOne>>")]
    #[serde(default)]
    pub pg: Option<Vec<PgConfig>>,
    #[serde(default)]
    pub allow_runtime_schema: bool,
    #[serde(default)]
    pub allow_runtime_interest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
    pub tls: Option<PgTlsConfig>,
    #[serde(default)]
    pub readonly: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgTlsConfig {
    pub cert_file: Utf8PathBuf,
    pub key_file: Utf8PathBuf,
    #[serde(default)]
    pub ca_file: Option<Utf8PathBuf>,
    #[serde(default)]
    pub verify_client: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuthzConfig {
    #[serde(alias = "bearer")]
    BearerToken(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipConfig {
    #[serde(alias = "addr")]
    pub bind_addr: SocketAddr,
    pub external_addr: Option<SocketAddr>,
    #[serde(default = "default_gossip_client_addr")]
    pub client_addr: SocketAddr,
    #[serde(default)]
    pub bootstrap: Vec<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub plaintext: bool,
    #[serde(default)]
    pub max_mtu: Option<u16>,
    #[serde(default = "default_gossip_idle_timeout")]
    pub idle_timeout_secs: u32,
    #[serde(default)]
    pub disable_gso: bool,
    #[serde(default)]
    pub member_id: Option<MemberId>,
    /// 广播传播时如何选目标 peer。研究用：random=现状基线，scored/rl=主动推送（见 docs/research）。
    #[serde(default)]
    pub broadcast_strategy: BroadcastStrategy,
    /// mission-aware 兴趣路由：表名 -> 关心该表的 peer gossip 地址列表。
    /// scored 策略据此给"关心这条 mutation 的 peer"加分（数据相关度）。
    /// 研究简化：兴趣由实验静态配置；动态 gossip 兴趣 profile 为后续工作。空=不启用。
    /// 注：被 `interest`(每节点自声明) + 复制表 `node_interest` 取代中；暂留作回退。
    #[serde(default)]
    pub interest_routing: std::collections::HashMap<String, Vec<SocketAddr>>,
    /// 本节点自声明的 interest：它关心哪些表。对应 4.2.3「数据需求模版」的节点自描述。
    /// 启动时写入复制表 `node_interest`，由全集群推送端/对账端读取。
    /// 空 = 关心全部表（= corrosion 现状全量复制行为，opt-in 关闭态）。
    #[serde(default)]
    pub interest: Vec<String>,
    /// 动态摘除某张表前，集群中必须仍有多少个其他在线且 ready(active=1)的 holder。
    /// 这是单次/串行 placement 变更的 fail-closed 门禁；并发摘除仍须由控制器串行化。
    #[serde(default = "default_interest_min_replicas")]
    pub interest_min_replicas: usize,
    /// 本节点 placement 配置的单调 fencing token。首次启用前 0=兼容模式；一旦写入非零
    /// epoch，后续每次改变 `interest` 必须严格递增，回到 0、旧 epoch 或同 epoch 不同
    /// placement 都会拒绝启动。epoch 模式的全量节点须显式使用 `interest = ["*"]`。
    #[serde(default)]
    pub interest_epoch: u64,
    /// 内嵌 GNN(4.3.3 活模型)配置。设置=在 agent 里加载训练好的权重、周期跑推理算
    /// 适配度评分 score(表,平台)驱动 selector 推送目标(仅 broadcast_strategy=rl 时生效)。
    /// 空=不启用(Rl 回退手设方差启发式)。
    #[serde(default)]
    pub graphrl: Option<GraphRlConfig>,
    /// critical(时效敏感)表:其广播跳过攒批延迟(缓冲立即 flush,不等 bcast_interval
    /// 500ms tick),每一跳 rebroadcast 同样生效 → 多跳时延从 O(跳数×攒批间隔) 降到
    /// O(跳数×RTT)。对应 4.3.3「优先将作战任务目标态势数据及时、精准推送」的"及时"。
    /// 与 graphrl.critical_tables(GNN 特征,模型面)独立:这里是传输优先级(数据面)。
    /// 空=不启用,全部表同等攒批。
    #[serde(default)]
    pub critical_tables: Vec<String>,
}

/// 内嵌 GNN 配置(任务模版级静态:权重路径 + 数据属性 + 角色映射 + critical)。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRlConfig {
    /// 训练好的 GNN 权重 JSON 路径(`research/rl/export_weights.py` 导出)。
    pub weights_path: String,
    /// 每张表的属性(全局,任务模版):写量/查询量,喂 GNN 数据特征。
    #[serde(default)]
    pub tables: Vec<GraphRlTable>,
    /// 角色→关心的表(有序,顺序须与训练 ROLES 一致:recon/strike/jam)。
    /// 用于从 peer 的 interest 反推其角色(平台 one-hot 特征)。
    #[serde(default)]
    pub roles: Vec<GraphRlRole>,
    /// critical(硬 SLA)表:其需要者视为 critical。MVP 简化;真实由战术规则定。
    #[serde(default)]
    pub critical_tables: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRlTable {
    pub name: String,
    pub write_vol: f32,
    pub query_vol: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphRlRole {
    pub name: String,
    pub tables: Vec<String>,
}

/// 广播传播的选 peer 策略。主动推送研究的总开关。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BroadcastStrategy {
    /// 现状基线：在候选 peer 中纯随机选 K 个。
    #[default]
    Random,
    /// 阶段1：按价值打分选 Top-K（数据相关度/链路/角色/负载）。重定向，不减量。
    Scored,
    /// 阶段1a 减量变体：候选池只留「关心该 mutation 的 peer」+ 少量覆盖配额，
    /// 任务无关平台不走快速推送 → 降全局推送量（对接 4.4.3）。无 interest 路由时退化为 Scored。
    ScoredReduce,
    /// 阶段3：图强化学习决策。
    Rl,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfConfig {
    #[serde(default = "default_huge_channel")]
    pub apply_channel_len: usize,
    #[serde(default = "default_big_channel")]
    pub changes_channel_len: usize,
    #[serde(default = "default_big_channel")]
    pub empties_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub to_send_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub notifications_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub schedule_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub clearbuf_channel_len: usize,
    #[serde(default = "default_mid_channel")]
    pub bcast_channel_len: usize,
    #[serde(default = "default_small_channel")]
    pub foca_channel_len: usize,
    #[serde(default = "default_wal_threshold")]
    pub wal_threshold_mb: usize,
    #[serde(default = "default_sql_tx_timeout")]
    pub sql_tx_timeout: usize,
    #[serde(default = "default_min_sync_backoff")]
    pub min_sync_backoff: u32,
    #[serde(default = "default_max_sync_backoff")]
    pub max_sync_backoff: u32,
    // How many unapplied changesets corrosion will buffer before starting to drop them
    #[serde(default = "default_processing_queue")]
    pub processing_queue_len: usize,
    // How many ms corrosion will wait before proceeding to apply a batch of changes
    // We wait either for apply_queue_timeout or untill at least apply_queue_min_batch_size changes accumulate
    #[serde(default = "default_apply_timeout")]
    pub apply_queue_timeout: usize,
    // Minimum amount of changes corrosion will try to apply at once in the same transaction
    #[serde(default = "default_apply_batch_min")]
    pub apply_queue_min_batch_size: usize,
    // batch_size = clamp(min_batch_size, step_base * 2 ** floor(log2(x/step_base)), max_batch_size)
    #[serde(default = "default_apply_batch_step")]
    pub apply_queue_step_base: usize,
    // Maximum amount of changes corrosion will try to apply at once in the same transaction
    #[serde(default = "default_apply_batch_max")]
    pub apply_queue_max_batch_size: usize,
    // Threshold ratio (0.0-1.0) for immediate batch spawning when queue reaches this fraction of batch size
    // It's used to decide whether to wait for more changes for apply_queue_timeout ms or spawn a batch immediately
    #[serde(default = "default_batch_threshold_ratio")]
    pub apply_queue_batch_threshold_ratio: f64,
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            apply_channel_len: default_huge_channel(),
            changes_channel_len: default_big_channel(),
            empties_channel_len: default_big_channel(),
            to_send_channel_len: default_mid_channel(),
            notifications_channel_len: default_mid_channel(),
            schedule_channel_len: default_mid_channel(),
            clearbuf_channel_len: default_mid_channel(),
            bcast_channel_len: default_mid_channel(),
            foca_channel_len: default_small_channel(),
            wal_threshold_mb: default_wal_threshold(),
            sql_tx_timeout: default_sql_tx_timeout(),
            min_sync_backoff: default_min_sync_backoff(),
            max_sync_backoff: default_max_sync_backoff(),
            processing_queue_len: default_processing_queue(),
            apply_queue_timeout: default_apply_timeout(),
            apply_queue_min_batch_size: default_apply_batch_min(),
            apply_queue_step_base: default_apply_batch_step(),
            apply_queue_max_batch_size: default_apply_batch_max(),
            apply_queue_batch_threshold_ratio: default_batch_threshold_ratio(),
        }
    }
}

fn default_gossip_idle_timeout() -> u32 {
    DEFAULT_GOSSIP_IDLE_TIMEOUT
}

pub const DEFAULT_GOSSIP_CLIENT_ADDR: SocketAddr =
    SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0u16, 0, 0));

fn default_gossip_client_addr() -> SocketAddr {
    DEFAULT_GOSSIP_CLIENT_ADDR
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Certificate file
    pub cert_file: Utf8PathBuf,
    /// Private key file
    pub key_file: Utf8PathBuf,

    /// CA (Certificate Authority) file
    #[serde(default)]
    pub ca_file: Option<Utf8PathBuf>,

    #[serde(default)]
    pub insecure: bool,

    /// Mutual TLS configuration
    #[serde(default)]
    pub client: Option<TlsClientConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsClientConfig {
    /// Certificate file
    pub cert_file: Utf8PathBuf,
    /// Private key file
    pub key_file: Utf8PathBuf,
}

pub fn default_admin_path() -> Utf8PathBuf {
    "/var/run/corrosion/admin.sock".into()
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    #[serde(default)]
    pub format: LogFormat,
    #[serde(default = "default_as_true")]
    pub colors: bool,
}

fn default_as_true() -> bool {
    true
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    #[error("gossip.max_mtu value {value} is below the QUIC minimum of 1200 (RFC 9000)")]
    InvalidMaxMtu { value: u16 },
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Reads configuration from a TOML file, given its path. Environment
    /// variables can override whatever is set in the config file.
    pub fn load(config_path: &str) -> Result<Self, ConfigError> {
        let config = config::Config::builder()
            .add_source(config::File::new(config_path, config::FileFormat::Toml))
            .add_source(config::Environment::default().separator("__"))
            .build()?;
        let cfg: Self = config.try_deserialize()?;
        cfg.validate()?;
        Ok(cfg)
    }
    fn validate(&self) -> Result<(), ConfigError> {
        if let Some(mtu) = self.gossip.max_mtu {
            if mtu < 1200 {
                return Err(ConfigError::InvalidMaxMtu { value: mtu });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ConfigBuilder {
    pub db_path: Option<Utf8PathBuf>,
    gossip_addr: Option<SocketAddr>,
    api_addr: Vec<SocketAddr>,
    endpoint_name: Option<String>,
    external_addr: Option<SocketAddr>,
    admin_path: Option<Utf8PathBuf>,
    prometheus_addr: Option<SocketAddr>,
    bootstrap: Option<Vec<String>>,
    log: Option<LogConfig>,
    schema_paths: Vec<Utf8PathBuf>,
    max_change_size: Option<i64>,
    consul: Option<ConsulConfig>,
    reaper: Option<ReaperConfig>,
    tls: Option<TlsConfig>,
    perf: Option<PerfConfig>,
    member_id: Option<MemberId>,
    max_mtu: Option<u16>,
    disable_gso: bool,
    allow_runtime_schema: bool,
    allow_runtime_interest: bool,
    gossip_interest: Vec<String>,
    gossip_interest_epoch: u64,
    broadcast_strategy: Option<BroadcastStrategy>,
}

impl ConfigBuilder {
    pub fn db_path<S: Into<Utf8PathBuf>>(mut self, db_path: S) -> Self {
        self.db_path = Some(db_path.into());
        self
    }

    pub fn gossip_addr(mut self, addr: SocketAddr) -> Self {
        self.gossip_addr = Some(addr);
        self
    }

    pub fn api_addr(mut self, addr: SocketAddr) -> Self {
        self.api_addr.push(addr);
        self
    }

    pub fn endpoint_name<S: Into<String>>(mut self, endpoint_name: S) -> Self {
        self.endpoint_name = Some(endpoint_name.into());
        self
    }

    pub fn prometheus_addr(mut self, addr: SocketAddr) -> Self {
        self.prometheus_addr = Some(addr);
        self
    }

    pub fn bootstrap<V: Into<Vec<String>>>(mut self, bootstrap: V) -> Self {
        self.bootstrap = Some(bootstrap.into());
        self
    }

    pub fn log(mut self, log: LogConfig) -> Self {
        self.log = Some(log);
        self
    }

    pub fn add_schema_path<S: Into<Utf8PathBuf>>(mut self, path: S) -> Self {
        self.schema_paths.push(path.into());
        self
    }

    pub fn admin_path<S: Into<Utf8PathBuf>>(mut self, path: S) -> Self {
        self.admin_path = Some(path.into());
        self
    }

    pub fn max_change_size(mut self, size: i64) -> Self {
        self.max_change_size = Some(size);
        self
    }

    pub fn consul(mut self, config: ConsulConfig) -> Self {
        self.consul = Some(config);
        self
    }

    pub fn reaper(mut self, config: ReaperConfig) -> Self {
        self.reaper = Some(config);
        self
    }

    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls = Some(config);
        self
    }

    pub fn member_id(mut self, member_id: MemberId) -> Self {
        self.member_id = Some(member_id);
        self
    }

    /// Set the maximum MTU for the QUIC gossip transport.
    pub fn max_mtu(mut self, mtu: u16) -> Self {
        self.max_mtu = Some(mtu);
        self
    }

    /// Disable Generic Segmentation Offload (GSO) for the QUIC gossip transport.
    pub fn disable_gso(mut self, disable: bool) -> Self {
        self.disable_gso = disable;
        self
    }

    /// Allow runtime (additive-only) DDL via `POST /v1/schema` (`api.allow_runtime_schema`).
    pub fn api_allow_runtime_schema(mut self, allow: bool) -> Self {
        self.allow_runtime_schema = allow;
        self
    }

    /// Allow runtime interest updates via `POST /v1/interest` (`api.allow_runtime_interest`).
    pub fn api_allow_runtime_interest(mut self, allow: bool) -> Self {
        self.allow_runtime_interest = allow;
        self
    }

    /// Set this node's self-declared interest (`gossip.interest`): the tables it cares about.
    pub fn gossip_interest<V: Into<Vec<String>>>(mut self, interest: V) -> Self {
        self.gossip_interest = interest.into();
        self
    }

    /// Set the interest epoch (`gossip.interest_epoch`) fencing placement changes.
    pub fn gossip_interest_epoch(mut self, epoch: u64) -> Self {
        self.gossip_interest_epoch = epoch;
        self
    }

    /// Set the broadcast strategy (`gossip.broadcast_strategy`).
    pub fn broadcast_strategy(mut self, strategy: BroadcastStrategy) -> Self {
        self.broadcast_strategy = Some(strategy);
        self
    }

    pub fn build(self) -> Result<Config, ConfigBuilderError> {
        let db_path = self.db_path.ok_or(ConfigBuilderError::DbPathRequired)?;

        let telemetry = TelemetryConfig {
            prometheus: self
                .prometheus_addr
                .map(|bind_addr| PrometheusConfig { bind_addr }),
            open_telemetry: None,
        };

        if self.api_addr.is_empty() {
            return Err(ConfigBuilderError::ApiAddrRequired);
        }

        Ok(Config {
            db: DbConfig {
                path: db_path,
                schema_paths: self.schema_paths,
                subscriptions_path: None,
                cache_size_kib: default_cache_size_kib(),
            },
            api: ApiConfig {
                bind_addr: self.api_addr,
                endpoint_name: self.endpoint_name,
                authorization: None,
                pg: None,
                allow_runtime_schema: self.allow_runtime_schema,
                allow_runtime_interest: self.allow_runtime_interest,
            },
            gossip: GossipConfig {
                bind_addr: self
                    .gossip_addr
                    .ok_or(ConfigBuilderError::GossipAddrRequired)?,
                external_addr: self.external_addr,
                client_addr: default_gossip_client_addr(),
                bootstrap: self.bootstrap.unwrap_or_default(),
                plaintext: self.tls.is_none(),
                tls: self.tls,
                idle_timeout_secs: default_gossip_idle_timeout(),
                max_mtu: self.max_mtu,
                disable_gso: self.disable_gso,
                member_id: self.member_id,
                broadcast_strategy: self.broadcast_strategy.unwrap_or_default(),
                interest_routing: Default::default(),
                interest: self.gossip_interest,
                interest_min_replicas: default_interest_min_replicas(),
                interest_epoch: self.gossip_interest_epoch,
                graphrl: None,
                critical_tables: Default::default(),
            },
            perf: self.perf.unwrap_or_default(),
            admin: AdminConfig {
                uds_path: self.admin_path.unwrap_or_else(default_admin_path),
            },
            telemetry,
            log: self.log.unwrap_or_default(),

            consul: self.consul,
            reaper: self.reaper,
        })
    }
}

fn default_interest_min_replicas() -> usize {
    1
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigBuilderError {
    #[error("db.path required")]
    DbPathRequired,
    #[error("gossip.addr required")]
    GossipAddrRequired,
    #[error("api.addr required")]
    ApiAddrRequired,
}

/// Log format (JSON only)
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[allow(missing_docs)]
pub enum LogFormat {
    #[default]
    Plaintext,
    Json,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct ConsulConfig {
    pub client: consul_client::Config,
}

// ReaperConfig specifies tables and the duration after which clock and pk records for deleted
// primary keys can be deleted (i.e data in <table>__crsql_pks and <table>__crsql_clock).
// WARNING: Specifying table to be reaped can cause inconsistencies if old primary keys come back
// after specified duration. Use with caution.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReaperConfig {
    pub tables: HashMap<String, TableReapConfig>,
    #[serde(default = "default_reaper_interval")]
    pub check_interval: usize,
    /// When false, skip scanning for PK rows with no clock entries.
    #[serde(default)]
    pub check_orphaned_pks: bool,
    /// Max orphaned PK rows to reap per table per tick.
    #[serde(default = "default_reaper_limit")]
    pub row_limit: usize,
}

/// Per-table reaper config.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TableReapConfig {
    pub retention: String,
    /// Optional WHERE clause fragment (without "WHERE") to only delete pks
    /// that match the filter e.g "id LIKE 'throwaway-%'"
    #[serde(default)]
    pub match_filter: Option<String>,
}
