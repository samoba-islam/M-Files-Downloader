# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please **do not** open a public GitHub issue.

Instead, please report it privately by emailing the maintainer directly or using [GitHub's private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability).

We will acknowledge your report within **48 hours** and aim to provide a fix or mitigation plan within **7 days**.

## Security Best Practices

When deploying this application:

- **Set a strong `FLASK_SECRET_KEY`** — do not use the default value.
- **Never commit `.env` files** containing real credentials.
- **Run behind a reverse proxy** (e.g., Nginx) with HTTPS in production.
- **Restrict network access** to the Flask port (default `5000`).
