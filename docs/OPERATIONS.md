# Operating guide

## Deployment contract

Spark dependencies are separate because they require Java and download Delta JARs. Local reference tests do not prove Spark runtime execution. Bronze and quarantine are append-only delivery logs and can contain replay duplicates; silver is idempotent by source key/version. Tables commit independently, so recovery replays a retained batch to finish gold publication. This bounded job is designed for batch CDC, not an implemented streaming connector.

Keep credentials in environment variables or a secret manager; `.env` is ignored. Example credentials, when present, are for isolated local development. No service in this portfolio is represented as an audited HIPAA/GDPR system.

## Change and recovery procedure

Run the tests before deploying. Start with synthetic fixtures, retain the input batch or event offset, and inspect rejected records before replaying. The source code and tests specify the idempotency keys and transactional boundaries. Do not bypass those boundaries to repair a failed load.

For a real service, define retention and backup policies, TLS, identity federation, per-role permissions, dependency patching, resource limits, and alert destinations. Containers and local files demonstrate deployment contracts; operating these integrations at scale requires load and failure testing in the target environment.

## Validation evidence

`VALIDATION.md` records checks actually executed when this portfolio was prepared. Integration commands requiring external services are explicitly distinguished from executed local tests. No availability, throughput, cost-saving, or compliance guarantee is inferred from unit tests.
