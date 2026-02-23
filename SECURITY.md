# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 1.0.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do NOT open a public GitHub issue for security vulnerabilities.**

Instead, email **krivamsh007@gmail.com** with:

1. A description of the vulnerability
2. Steps to reproduce the issue
3. The potential impact
4. Any suggested fix (optional)

You should receive an acknowledgement within **48 hours**. A fix will be prioritized based on severity:

| Severity | Target Response |
|----------|-----------------|
| Critical (credential exposure, RCE) | Patch within 24 hours |
| High (auth bypass, data leak) | Patch within 72 hours |
| Medium (privilege escalation) | Patch within 1 week |
| Low (information disclosure) | Next release cycle |

## Credential Handling

This project interacts with Azure AD service principals and Microsoft Fabric APIs. Follow these practices:

- **Never commit `.env` files.** The `.gitignore` excludes all `.env` variants except `.env.template`.
- **Use placeholder values** in `.env.template` — never real secrets.
- **Rotate secrets immediately** if accidentally exposed. Revoke the client secret in Azure AD and generate a new one.
- **Use `SecretStr`** (Pydantic) for any field that holds a credential. This prevents secrets from appearing in logs, stack traces, or serialized output.
- **Scope service principal permissions** to the minimum required — workspace Member, not Tenant Admin.

## Token Scopes

This project uses two distinct OAuth scopes. Do not conflate them:

| API | Scope | Purpose |
|-----|-------|---------|
| Fabric REST API | `https://api.fabric.microsoft.com/.default` | Workspace, item, and model operations |
| OneLake DFS API | `https://storage.azure.com/.default` | File upload/download to lakehouse storage |

## Dependencies

- Dependencies are pinned to minimum versions in `pyproject.toml`
- `pydantic<2.10` is intentionally constrained for Fabric Spark runtime compatibility
- Run `pip audit` periodically to check for known vulnerabilities in dependencies

## Disclosure Policy

- Confirmed vulnerabilities will be fixed in a patch release
- A security advisory will be published via GitHub Security Advisories
- Credit will be given to the reporter (unless they prefer anonymity)
