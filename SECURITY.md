# Security Policy

## Supported versions

This project is pre-1.0. Security fixes land on `master` and in the next tagged release. There is no
backport branch yet.

| Version | Supported |
|---------|-----------|
| `master` | yes |
| `v0.1.x` | yes |
| older | no |

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Use GitHub's private vulnerability reporting: go to the **Security** tab of this repository and
choose **Report a vulnerability**. That opens a private channel visible only to the maintainers.

If that is unavailable, email **contact@smartdataengines.com** with `SECURITY` in the subject and:

- a description of the issue and the impact you believe it has
- steps to reproduce, ideally a minimal input or a small program
- the commit SHA or release you tested
- your build configuration (compiler, build type, CMake options)

## What to expect

| Stage | Target |
|-------|--------|
| Acknowledgement of your report | 3 working days |
| Initial assessment (severity, affected versions) | 10 working days |
| Fix or documented mitigation | depends on severity; we will keep you updated |

We will credit you in the release notes unless you prefer to stay anonymous.

## Scope

In scope: memory-safety bugs, crashes triggerable by untrusted input, protocol-parsing flaws in the
TCP or multi-master wire protocol, authentication or authorisation bypasses in replication and
failover paths, denial of service reachable from a network client, data corruption or loss.

Out of scope: results that require a modified build, findings on deliberately unsafe configurations
documented as such, missing hardening flags without a demonstrated impact, and reports produced by a
scanner without a reproducible case.

## Security posture you should know about

This engine is designed for deployment on a **trusted network**. As of `v0.1.x` the TCP wire
protocol has **no authentication and no transport encryption**. Do not expose `ob_tcp_server`,
the replication port, the multi-master port, or the metrics port to the public internet. Put them
behind a private network, a VPN, or a mutually authenticated proxy.

If you are evaluating this engine for production, treat that as a deployment requirement and not a
vulnerability. Adding authentication and TLS is on the roadmap (`docs/roadmap.md`, phase 7).
