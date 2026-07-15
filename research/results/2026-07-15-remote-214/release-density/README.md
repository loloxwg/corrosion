# Release density result index

These files validate the per-node 10K QPS acceptance boundary with
`target/release/corrosion`. The load generator and agents ran on the same
20-core Linux host. Detailed interpretation is in
[`docs/research/2026-07-15-remote-100-node-validation.md`](../../../../docs/research/2026-07-15-remote-100-node-validation.md).

- `qps-release-1node-8workers.json`: isolated release agent, 36,808 QPS.
- `qps-release-4nodes-4workers-10s-r{1,2,3}.json`: stable density proof; all
  four nodes exceeded 10K in all three runs.
- `qps-release-5nodes-4workers-10s*.json`: threshold probe; one of three runs failed.
- `qps-release-10nodes-4workers-10s.json` and
  `qps-release-20nodes-4workers-10s.json`: saturation points.

Release binary SHA-256:

```text
3e74a5b4440ed296af95901a3de5ceea45de901478f1c450eb97c67056378a2d  target/release/corrosion
```
