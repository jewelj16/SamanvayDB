# SamanvayDB on Windows with Docker Desktop

This is the supported Windows workflow for this repository. Run the cluster
and `kvctl` in Docker; no local Go installation, GNU Make, or Windows-built
executables are required.

## Prerequisites

Install Docker Desktop for Windows and configure it to use **Linux
containers** and the **WSL 2** engine. Start Docker Desktop before using this
repository. In PowerShell, this command can launch it when it is installed in
the default location:

```powershell
& "$env:ProgramFiles\Docker\Docker\Docker Desktop.exe"
```

Wait for Docker Desktop to report **Engine running**, then verify that both a
client and a server are shown:

```powershell
docker version
docker context ls
```

The expected context is `desktop-linux`. If `docker version` reports that it
cannot connect to `dockerDesktopLinuxEngine`, Docker Desktop is not ready yet.
Use **Docker Desktop -> Troubleshoot -> Restart Docker Desktop**, wait for the
engine to start, and run `docker version` again. Do not run Compose until a
`Server:` section is present.

If the Docker engine does not start, check and refresh WSL 2, then restart
Docker Desktop:

```powershell
wsl --status
wsl --update
wsl --shutdown
```

`Default Version: 2` is correct. A WSL1 warning is not relevant to this
Docker Desktop workflow.

## Build and start the cluster

From the repository root, Docker Compose builds the SamanvayDB image and
starts the standard nine-node, three-shard cluster with three replicas per
shard:

```powershell
docker compose -f docker/docker-compose.yml up -d --build
docker compose -f docker/docker-compose.yml ps
```

Wait for the Raft leaders to be elected before issuing requests. This
PowerShell loop waits until every shard reports a leader:

```powershell
$composeFile = 'docker/docker-compose.yml'
do {
    Start-Sleep -Seconds 1
    $status = docker compose -f $composeFile exec -T node-1 kvctl --server node-1:7000 --output json cluster status 2>$null
} until ($LASTEXITCODE -eq 0 -and $status -match '"leader": "node-' -and $status -notmatch '"leader": ""')
$status
```

## Important: run `kvctl` inside Docker

The cluster advertises internal addresses such as `node-1:7000`. Those names
are available only on the Compose network. Therefore, use `docker compose exec`
for `kvctl` commands instead of `bin\kvctl.exe` on the Windows host.

For convenience, the examples below use this command prefix:

```powershell
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000
```

## Cluster status and diagnostics

```powershell
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 cluster status
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 shard status --shard 0
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 node status --node node-1
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 cluster metrics

# JSON is useful for scripts.
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 --output json cluster status

# Container state and server logs.
docker compose -f docker/docker-compose.yml ps
docker compose -f docker/docker-compose.yml logs -f node-1
```

The HTTP endpoint of node 1 is exposed to Windows on port 8001:

```powershell
Invoke-RestMethod http://127.0.0.1:8001/healthz
Invoke-RestMethod http://127.0.0.1:8001/status
```

## Write and read data

```powershell
# Write a key and read it back. Default reads are linearizable.
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 put user:42 alice
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 get user:42

# A follower read can be stale.
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 get user:42 --consistency follower-ok

# TTL, compare-and-swap, and delete.
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 put session:abc active --ttl 30s
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 cas user:42 alice alice-smith
docker compose -f docker/docker-compose.yml exec node-1 kvctl --server node-1:7000 delete session:abc
```

`get` returns exit code `3` when a key does not exist. Add `--timeout 20s` to
a command to extend the default request timeout.

## Stop or reset the cluster

Stop containers but retain their data volumes:

```powershell
docker compose -f docker/docker-compose.yml down
```

Stop containers and delete all SamanvayDB Docker data, returning to a clean
cluster on the next `up`:

```powershell
docker compose -f docker/docker-compose.yml down -v
```
