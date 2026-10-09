# Security notes

Before reporting a potential vulnerability, reproduce it with synthetic data and
omit credentials, private paths and file contents. For a vulnerability requiring
sensitive details, use GitHub private vulnerability reporting if enabled; do not
post exploit details publicly before discussing disclosure with the maintainer.

Version 0.1 is a portfolio release. There is no service-level commitment or formal
security audit. CI runs dependency advisory checks with pip-audit, and Dependabot
tracks Python packages and workflow actions. Advisory checks cannot establish the
absence of application vulnerabilities. Review the documented filesystem, retry
or provider boundaries before using this project with important data.
