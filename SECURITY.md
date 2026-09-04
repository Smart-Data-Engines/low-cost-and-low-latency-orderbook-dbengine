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

This engine is designed for deployment on a **trusted network**, and the reason is now narrower than
it was: client sessions can authenticate, and nothing is encrypted.

### What authentication gives you

`--auth-secret-file` turns on authentication for client sessions. A session that has not
authenticated may run only `AUTH`, `PING` and `QUIT`; everything else is refused with
`ERR unauthenticated`. The scheme is challenge-response over HMAC-SHA256, so **the secret never
crosses the wire**:

```
C: AUTH
S: OK CHALLENGE <64 hex characters>
C: AUTH <identity> <HMAC-SHA256 of the challenge>
S: OK AUTH <identity>
```

A failed attempt closes the connection, which is also the rate limit: one attempt per connection,
and connections are bounded by `--max-sessions`. Successes and failures are logged with the peer
address and counted in `ob_auth_success_total` and `ob_auth_failures_total`.

`--cluster-secret-file` holds the secret for the replication and multi-master links. **The two files
must not contain the same secret** and the server refuses to start when they do: a client holding
the cluster secret could present itself as a replica and stream the entire write-ahead log.

Generating and installing them is in [docs/operations.md](docs/operations.md).

### What authentication does not give you

**Not confidentiality on its own.** Authentication answers "may this peer talk to us"; it says
nothing about who else is listening. **Client sessions can now be encrypted** with `--tls-client`
(TLS 1.3, certificate verification on by default in our clients) — see
[docs/operations.md](docs/operations.md). The **replication link and the multi-master mesh are still
plaintext**: they authenticate, and every record they carry is readable on the path. If your threat
model includes a passive observer, encrypt the client port and keep the cluster links on a network
you trust until the cluster half of #30 part three lands.

**Not authorisation.** Every authenticated identity may run every command, including `FAILOVER` and
`MIGRATE`. Per-identity permissions are roadmap item #31, and they will attach to exactly the
identity name this scheme establishes.

**Not protection against an active man-in-the-middle, on the links that are not encrypted**, and
this one is worth understanding rather than glossing. On a TLS client session with verification on,
the channel is bound to a certificate and the relay below does not apply — **provided the client
checks the name and not only the chain.** Both of ours do: `tls_verify=True` requires the
certificate to cover the address dialled. Without that, a private CA signing a whole cluster makes
every node's certificate acceptable for every other node, so the relay works again between two
holders of legitimate certificates, and every verification reports success. Chain verification alone
is not peer verification. On the cluster links,
which are not yet encrypted, it does. Challenge-response proves *knowledge of the secret*; nothing binds the exchange to
the connection it happened on. So an attacker who can redirect a replica's connection — by
controlling DNS, ARP or routing — can relay: it takes the real primary's challenge, presents it to
the replica as its own, forwards the replica's answer to the primary, and asks the primary to answer
the replica's challenge. Both sides then believe they are talking to each other.

Channel binding is what stops that, and channel binding needs a channel with an identity — which is
TLS. The client port has it since #30 part three; the cluster links do not yet, and that is the
remaining half of that item. It is not fixable at the authentication layer: a relay can always
forward a value that is bound to nothing but a nonce.

In practice this changes little about where you may deploy a node, because an attacker with that
level of network control already reads every row in the clear and can inject records into the
plaintext stream, authentication or not. What it does mean is that **authentication is not a reason
to move a node onto a network you do not trust.**

**Nothing on `/metrics`.** The endpoint has no authentication and that is deliberate: a Prometheus
scraper cannot perform a challenge-response, so a bearer token would be a second and weaker
mechanism — and the weaker one is the one that gets used. Bind it to a private interface with
`--metrics-bind` instead. Note that the exposition discloses symbol counts, row volumes and cluster
role, which is information about your business.

**Nothing at all, by default.** With neither secret file set, the wire behaves exactly as it did
before: no authentication anywhere. The server logs a `WARN` for each disabled surface at startup, so
"default open" is visible in the log rather than only in this document.

### Therefore

Do not expose `ob_tcp_server`, the replication port, the multi-master port, or the metrics port to
the public internet, authenticated or not. Put them behind a private network, a VPN, or a mutually
authenticated proxy. If you are evaluating this engine for production, treat that as a deployment
requirement and not a vulnerability.
