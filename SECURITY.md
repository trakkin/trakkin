# Security Policy

## Supported Versions

Trakkin is in rapid development. Security fixes are provided for the latest stable release only. Older releases, release candidates, development snapshots, are not guaranteed to receive fixes.

| Version | Supported |
| ------- | --------- |
| 1.0.x   | Yes       |
| < 1.0   | No        |

Upgrade to the latest release before reporting a vulnerability when practical. If the issue only exists on the `next` branch, include the affected commit SHA.

## Reporting a Vulnerability

Do not open a public issue or pull request for a suspected vulnerability. Submit it privately through [GitHub Security Advisories](https://github.com/trakkin/trakkin/security/advisories/new).

Include as much of the following as possible:

- The affected release or commit SHA.
- The affected component and deployment configuration.
- The security impact and who could exploit it.
- Minimal reproduction steps or a proof of concept.
- Relevant logs with tokens, credentials, and personal media data removed.
- Any known mitigations or suggested fixes.

## Disclosure Process

Maintainers will acknowledge the report as soon as practical, validate its impact, and coordinate remediation and disclosure with the reporter. Please do not disclose the vulnerability publicly until a fix or mitigation has been
published and coordinated disclosure is complete.
