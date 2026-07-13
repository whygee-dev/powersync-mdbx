# Security

The repository does not provide deployment hardening, secret distribution, backup/restore automation, rolling upgrades, or an incident-response service. Deployments must supply those controls before exposing the service to an untrusted network.

The code performs authentication and data routing, so defects in it are in scope for reporting. `/sync/stream` fails closed without configured JWT verification keys unless `POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC=1` is explicitly set for local benchmarking. Admin routes require an API token.

## Reporting

Use GitHub's **Security → Report a vulnerability** flow. If private vulnerability reporting is unavailable, email `whygee.dev@gmail.com`. Do not open a public issue for a defect that exposes data or credentials.

Include the affected commit, configuration, impact, and a minimal reproduction. There is no bug bounty or guaranteed response time. Reports will be acknowledged and assessed as maintainer availability permits; credit is offered unless anonymity is requested.

## Known boundary

TCP PostgreSQL URIs must explicitly select `sslmode=verify-full` or `sslmode=disable`. For continuous replication, `verify-full` uses the compiled WebPKI root set when `sslrootcert` is absent; a supplied `sslrootcert` replaces that set and must contain at least one certificate. Exported-snapshot creation uses libpq through `pg_walstream`, so its default root-certificate lookup follows libpq configuration. Client certificates require a paired `sslcert` and `sslkey`. Plaintext mode is intended only for a deliberately trusted private transport. Unsupported weaker modes fail at startup.

When JWT verification keys are configured, startup also requires non-empty accepted audience and issuer lists. Tokens require an expiry and an exact matching audience and issuer. Anonymous sync is available only through the explicit `POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC=1` benchmark/development switch.

Remote JWKS are loaded once at startup. Their URLs must use HTTPS; HTTP is accepted only for `localhost` or a loopback IP address in explicit development configurations. Redirects from HTTPS remain HTTPS. A loopback HTTP URL may redirect to HTTPS or another loopback HTTP URL.

Request bodies, bucket counts, stream lifetimes, concurrent sync reads, retained tail history, and per-read entry/data bytes are bounded. Table-backed parameter lookups are materialized into MDBX during replication and resolved with in-process reads at request time, bounded by the same per-request bucket-count cap (`POWERSYNC_RUST_MAX_BUCKETS_PER_REQUEST`, default 10,000), which errors rather than truncates past the limit; no PostgreSQL connection is opened on the `/sync/stream` path.

Online layout-changing rule activation is deliberately rejected. Removing all persisted state outside the managed bootstrap path also removes cursor-epoch history; operators must treat that as a client cursor reset. These documented limitations do not need private reporting unless a report adds a materially different exploit or impact.
