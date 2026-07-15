# External load-generator result index

The Corrosion release agents ran on `192.168.3.214` while the load generator
ran on a separate Darwin arm64 host over the physical `192.168.3.0/24` LAN.
The accepted measurements use oha 1.15.0:

- `oha-release-external-node0-c64-10s.json`: one agent, 39,234 QPS, 100% success.
- `oha-4nodes-c64-r1/`: four agents, minimum 11,602 QPS, 4/4 pass.
- `oha-5nodes-c64-r1/`: five agents, minimum 9,266 QPS, 0/5 pass.
- `oha-10nodes-c64-r1/`: ten agents, minimum 4,680 QPS, 0/10 pass.

oha binary SHA-256:

```text
ca53b088d4bc79778948ba36a4766ca0ebc55a3b92b2c090e07161184380a737  oha-1.15.0-darwin-arm64
```

Discarded diagnostic experiments showed that Python multiprocessing saturated
around 29K aggregate, while ApacheBench either measured TCP handshake
throughput or encountered connection resets against the streaming response.
Those tool-limited runs are deliberately not retained as acceptance evidence.
