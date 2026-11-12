# Zenon P2P Observer

Independent Zenon momentum collection with a read-only JSON API for
[go-zenon-version](https://0x3639.com/go-zenon-version/).
Release **0.3.0-associations** adds cohort comparisons, delivery coverage, matched
relay baselines, chronological holdouts and 90-day summaries to the continuous
collector and read-only API. The existing Observatory
and discovery service remain independent and unchanged.

**A relay's advertised version is not its producer's software version.** Producer
addresses derive from checked signing keys, without consensus validation. No
ownership or software attribution is inferred from timing. Conflicting signer
claims are disputed and their producer address is withheld.

The [original feasibility test](docs/feasibility-report.md) passed with 337 checked
momentums seen from two peers over an hour. The collector still needs the four-peer,
24–48 hour remote pilot in [SPEC.md](SPEC.md). Association analysis is implemented; see [method and upgrade instructions](docs/association-analysis.md).
Remote resource/rollback acceptance, reviewed identity assertions and dashboard
integration remain separate work. A collection uptime report does not certify
those pilot checks or establish producer versions.

## Deploy in Coolify

Use a Git-based **Docker Compose** application, repository root `/`, Compose file
`/compose.yaml`, one replica. Save/load the definition after upgrading.

Before upgrading 0.2.0, run `observer backup pre-associations-2026-11-11.sqlite`
in the current container Terminal. Schema 2 needs this backup for rollback to 0.2.0.
Keep the volume, `OBSERVER_ID`,
peer seeds and traffic limits. Update these environment variables:

```dotenv
RUN_DURATION=0s
MAX_PEERS=4
RAW_RETENTION_DAYS=7
ALLOWED_ORIGINS=https://0x3639.com
```

`RUN_DURATION=0s` enables continuous service. An old `65m` value still stops it after
65 minutes. Supply at least four distinct public peer identities on distinct IPs
for the four-peer pilot; increasing MAX_PEERS does not create missing seeds.
[Four candidates used in the local check](docs/pilot-peers.env.example) are provided.
Use `enode://<128-character-key>@<host>:<port>` entries in `PEER_SEEDS`, separated by
commas. There is no automatic discovery. See [.env.example](.env.example).

Assign the `observer` service your HTTPS domain and **internal port 8080**, with
an empty path. The selected domain for this deployment is:

`https://zsjo46q7s0qcmaklbmak7gfp.172.245.238.165.sslip.io`

No P2P listener or direct host port is needed. ALLOWED_ORIGINS contains origins
without paths; empty disables browser cross-origin access. Setting CORS does not
modify the existing website.

Deploy and check `/healthz`, `/readyz`, and `/api/v1/status`. Logs include startup
and compact status every 30 seconds. The health check tests liveness; peer/RPC
outages do not trigger restarts. Readiness is separate. See
[operations](docs/operations.md) for pause states, backups, and rollback.

Compose retains `observer-data:/data`, 512 MiB, one CPU, no extra swap, and uses
`on-failure:3`. The 2 GiB data budget includes WAL, sidecars, backups and old
experiment files; it is not a filesystem quota. Prefer an isolated host or a
Coolify build server. Images can be built elsewhere and deployed by registry digest.

## API

| Route | Result |
| --- | --- |
| `/healthz` | Process liveness |
| `/readyz` | Initialization/write readiness, or 503 |
| `/api/v1/status` | Collection, references, queue, storage and traffic |
| `/api/v1/peers` | Historical sessions, endpoints and advertised versions |
| `/api/v1/momentums` | Checked headers, producer addresses and verification |
| `/api/v1/momentums/{hash}/arrivals` | Original per-peer arrival evidence |
| `/api/v1/pillars/{address}` | Dated name snapshots and observed momentums |
| `/api/v1/associations` | Cohort-specific relay comparisons, daily views, baselines and holdouts |

[API guide](docs/api.md) · [OpenAPI](docs/openapi.json) ·
[Interpretation guidance](docs/interpretation.json)

## Local build and test

All dependencies and execution run in containers. No Node tooling is needed.

```sh
./scripts/check.sh
cp .env.example .env
# Set OBSERVER_ID and PEER_SEEDS.
docker compose -f compose.yaml -f compose.preview.yaml up -d --build
curl http://127.0.0.1:8080/api/v1/status
```

The preview override publishes localhost only. `compose.local.yaml` is a
workstation-only override using a prebuilt binary and workspace data; never select
it in Coolify. Go 1.26, base images and checksums are pinned. The upstream revision
remains `bb39e31c608dc397a91b98b21b096bca99a9f4d6`. Checks include races, compatibility,
static analysis and compilation. The full upstream server fixture runs separately
because of its documented lifecycle race; production uses our transport adapter.

## Existing experiment data

Upgrading preserves `identity.key`, `limits.json`, and old JSONL/reports. Service
mode creates SQLite alongside them. Legacy events are **not imported into the API**
or counted toward the new pilot. This is disclosed in data quality. No private key
is served or logged.

To run a separate finite CLI experiment without HTTP:

```sh
docker compose -f compose.yaml -f compose.feasibility.yaml up -d --build
```

Never run both modes against the same volume concurrently.
