# Raw result index

Environment and interpretation are documented in
[`docs/research/2026-07-15-remote-100-node-validation.md`](../../../docs/research/2026-07-15-remote-100-node-validation.md).

Tracked machine-readable results:

- `qps-100nodes.json`: 100 agents, one load process per agent, 500,000 requests.
- `qps-100nodes-2workers.json`: 100 agents, two load processes per agent, 500,000 requests.
- `qps-100nodes-4workers.json`: 100 agents, four load processes per agent, 500,000 requests.
- `qps-1node-8workers.json`: one agent, eight load processes, 160,000 requests.
- `qps-smoke-6nodes.json`: load-driver smoke test.
- `release-density/`: release single-node and 4/5/10/20-agent density validation.
- `release-external/`: independent Darwin load-generator validation over the
  physical LAN, including accepted oha single/4/5/10-agent measurements.

The adjacent `.log` files are preserved in the working tree and on
`192.168.3.214:/home/xwg/dev/corrosion-active-push-validation/research/results/2026-07-15-remote-214/`,
but are excluded from Git by the repository-wide `*.log` rule.

SHA-256 captured on the remote host:

```text
ae8aebf2937f8f2160d541c25fe41a9458c07623e70c1f35ad22ffa382b03518  phase2-100nodes-v2.log
d145cd102365dbc6df3a4469afb07d469d590fe19b94ce4f5ed1002bbe7a3919  phase2-100nodes.log
149d7c7ae82833c62784e893772a84664ab579af01a3bc746fc85f2379480bd4  qps-100nodes.json
731341bdbf6101af400d69878c21dfab24496fb6ec8684fbaab08cb6a218847a  qps-100nodes.log
47fbd3925cfe43f9cd965c3220c1e936a77fa311e04ac2e2fb43c3961c802b44  qps-1node-8workers.json
8e060493509797afa279c3d5f855593111511f6f7e7b47374836ad81557c31d3  qps-1node-8workers.log
989a5a39a2a80adaf167131efbba728fbf29bb84163a3e7034b026c943a011fd  qps-smoke-6nodes.json
a9938df2df258c4659fe33376621c06173946c465c757666642e0c5305e14e49  transfer-100nodes.log
e7ea4e652b0397fbdf345f19066b69ccb20c6223190633f09880e9947631f2aa  qps-100nodes-2workers.json
49f69325df5606c315083eef7739f4e61abb04bfd31e15923aa7700ce3a1caff  qps-100nodes-2workers.log
6c6b3b087fc45dce576aeb63b33d000deb5e55b44bd1f6cec9c7c131a4b2e934  qps-100nodes-4workers.json
8ba102c9a1621fa6d5214b0284787152ba1a55fa4aa1d5833014e674324361ce  qps-100nodes-4workers.log
```
