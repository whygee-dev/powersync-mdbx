# Security

No version of this prototype is supported for production or internet-facing deployment.

The code handles authentication and data routing, so security defects still matter. `/sync/stream` fails closed without configured JWT verification keys unless `POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC=1` is explicitly set for local benchmarking. Admin routes require an API token.

## Reporting

Use GitHub's **Security → Report a vulnerability** flow after the repository is public. If private vulnerability reporting is unavailable, email `whygee.dev@gmail.com`. Do not open a public issue for a defect that exposes data or credentials.

Include the affected commit, configuration, impact, and a minimal reproduction. There is no bug bounty or guaranteed response time. Reports will be acknowledged and assessed as maintainer availability permits; credit is offered unless anonymity is requested.

## Known boundary

TCP PostgreSQL URIs must explicitly select `sslmode=verify-full` or `sslmode=disable`. Verification uses system roots when `sslrootcert` is absent; a supplied `sslrootcert` replaces that trust set and must contain at least one certificate. Client certificates require a paired `sslcert` and `sslkey`. Plaintext mode is intended only for a deliberately trusted private transport. Unsupported weaker modes fail at startup.

When JWT verification keys are configured, startup also requires non-empty accepted audience and issuer lists. Tokens require an expiry and an exact matching audience and issuer. Anonymous sync is available only through the explicit `POWERSYNC_RUST_ALLOW_ANONYMOUS_SYNC=1` benchmark/development switch.

Request bodies, bucket counts, stream lifetimes, concurrent sync reads, parameter-query concurrency/time/rows, retained tail history, and per-read entry/data bytes are bounded. Parameter queries still establish one bounded source connection per evaluation rather than using a pool. The constrained compiler and protocol surface have not received an independent security assessment, and the project is not supported for internet-facing use.

Online layout-changing rule activation is deliberately rejected. Removing all persisted state outside the managed bootstrap path also removes cursor-epoch history; operators must treat that as a client cursor reset. These documented limitations do not need private reporting unless a report adds a materially different exploit or impact.
