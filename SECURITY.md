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
it was: all three surfaces can authenticate, and all three can be encrypted.

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
nothing about who else is listening. Encryption is a separate switch per surface, and all three
exist: `--tls-client` for client sessions, `--tls-replication` for the replication link,
`--tls-multi-master` for the mesh (TLS 1.3 throughout, no configurable floor) — see
[docs/operations.md](docs/operations.md). Every one of them is **off by default**, and a node with
none of them carries every query and every row in the clear.

**Authorisation is per identity, and opt-in** (#31). With `--auth-acl-file` each identity of the
secret file has a role and, optionally, the symbols and exchanges it reaches:
- `read` runs the queries;
- `write` runs the writes as well;
- `admin` runs everything, `FAILOVER`, `MIGRATE` and `BACKUP` among it.

A refusal is `ERR forbidden: ...` and leaves the session open. **Without the file every identity
may run every command**, which is how every node before #31 behaved. The server refuses a
permissions file that anyone but its owner - the server's user or root - may write. See "Permissions" in
[docs/operations.md](docs/operations.md).

**Not protection against an active man-in-the-middle — that comes from TLS, and only when the name
is checked as well as the chain.** This is worth understanding rather than glossing, because it is
the reason encryption is not merely about eavesdropping.

Challenge-response proves *knowledge of the secret* and nothing more: no part of the exchange is
bound to the connection it happened on. So on an unencrypted link an attacker who can redirect a
replica — by controlling DNS, ARP or routing — relays. It takes the real primary's challenge,
presents it to the replica as its own, forwards the replica's answer to the primary, and asks the
primary to answer the replica's challenge. Both sides then believe they are talking to each other.
This is not fixable at the authentication layer: a relay can always forward a value bound to nothing
but a nonce.

Channel binding is what stops it, and channel binding needs a channel with an identity. So:

| Surface | With no TLS | With TLS |
|---|---|---|
| client port | relay works | stopped, because our clients require the certificate to cover the address they dialled |
| replication link | relay works | stopped: TLS there is **mutual** and cannot be configured otherwise |
| multi-master mesh | relay works | stopped, same |

**The name check is the load-bearing half, and chain verification is not peer verification.**
`SSL_VERIFY_PEER` answers "did a trusted CA sign this certificate" and says nothing about whose it
is. With a private CA signing a whole cluster — how anyone deploys this — chain-only verification
makes every node's certificate acceptable for every other node, the relay works again *between two
holders of legitimate certificates*, and every check reports success. Both of our clients check the
name; on the node links the dialling end checks the name it dialled, and the accepting end verifies
the chain plus, when `--tls-peer-names` is given, an explicit identity allowlist. Without that
allowlist the accepting end accepts any identity your CA signed, which is correct when the CA signs
nothing but the cluster and wrong when it is a corporate CA — the startup log says which mode is in
force, and `ob_mm_peers_tls_verified` against `ob_mm_peers_connected` says how many live peers are
actually verified — with `ob_replicas_tls_verified` against `ob_replicas_connected` for the
replication link.

A node link needs a trust anchor (`--tls-ca-file`) and the process refuses to start without one.
There is deliberately no "encrypt but do not verify the peer" mode for those links: it would leave
this relay open while looking like protection.

**A certificate is replaced without a restart** (#238). `SIGHUP`, which `systemctl reload` sends,
reads the certificate, the key and the CA again. The files are held to the start's rules: a key
readable beyond its owner, or one that does not match its certificate, is refused. A refused reload
leaves the certificate in force rather than a node without one, and every surface changes or none
does. Connections already open keep the certificate they were opened with.
`ob_tls_certificate_not_after_seconds` says when the one in force expires, and the shipped rules
alert two weeks before. A reload does not read `--tls-peer-names`, so taking one node's identity out
of a cluster is still a restart. [docs/operations.md](docs/operations.md), "Replacing a
certificate".

What has not changed: an attacker with that level of network control over a **plaintext** cluster
link already reads every row and can inject records into the stream, authentication or not. So
**authentication alone is not a reason to move a node onto a network you do not trust** — with TLS
and mutual verification on the node links, it is.

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
