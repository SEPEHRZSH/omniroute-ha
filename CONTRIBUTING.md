# Contributing

Contributions are welcome, especially reproducible failover tests, SQLite compatibility findings, deployment documentation, and independent multi-host validation.

## Reporting an issue

Include the OmniRoute and Node.js versions, operating system, MongoDB/Redis topology, steps to reproduce, expected behavior, actual behavior, and relevant sanitized logs. Never include credentials, API keys, real provider accounts, database dumps, or private hostnames.

## Submitting changes

Fork the repository, create a focused branch, and open a pull request explaining the behavior change and the tests performed. Preserve the pinned upstream checkout, keep changes scoped, and add regression coverage for behavioral changes. Run the relevant checks described in INSTALL.md once the source import is complete. Do not claim support for untested SQL operations or production topologies.

## Security

Do not post secrets or exploitable details publicly. Use GitHub private vulnerability reporting if enabled, or contact the maintainer privately before disclosing security-sensitive findings.
