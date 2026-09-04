# What TLS would cost this engine

Two standalone programs, run once to decide the shape of #30 part three. They are here because a
number that cannot be re-measured is an anecdote, and this one governs a design decision:
`kiro-workspace/specs/wire-tls/requirements.md` §1.

Standalone, and not part of `bench_engine`: neither of them touches the engine. They measure what
OpenSSL and this kernel do with a socket, which is the question that had to be answered *before*
writing TLS into four I/O loops.

## `ktls_probe.c` — does this build negotiate kernel TLS?

```bash
openssl req -x509 -newkey rsa:2048 -keyout /tmp/key.pem -out /tmp/cert.pem \
        -days 1 -nodes -subj "/CN=localhost"
cc -O2 -o /tmp/ktls_probe benchmarks/tls/ktls_probe.c -lssl -lcrypto -lpthread
/tmp/ktls_probe /tmp/cert.pem /tmp/key.pem
```

Prints `BIO_get_ktls_send` and `BIO_get_ktls_recv` for both ends. Asked because Ubuntu's OpenSSL
is not built with `enable-ktls` on every release, and "the kernel has `CONFIG_TLS`" does not answer
it. Measured here, OpenSSL 3.0.13 on 6.8.0:

| | kTLS send | kTLS recv |
|---|---|---|
| TLS 1.3, `TLS_AES_256_GCM_SHA384` | **1** | 0 |
| TLS 1.2, `ECDHE-RSA-AES128-GCM-SHA256` | **1** | **1** |

So a full kernel data path needs TLS 1.2, and part three chooses 1.3 anyway — the reasoning is in
the spec. To see the 1.2 row, set `SSL_CTX_set_max_proto_version(ctx, TLS1_2_VERSION)` and a 1.2
cipher list.

## `tls_cost.c` — what the record layer costs at this protocol's message sizes

```bash
cc -O2 -o /tmp/tls_cost benchmarks/tls/tls_cost.c -lssl -lcrypto -lpthread
/tmp/tls_cost 0 61440 3050                                   # plaintext
/tmp/tls_cost 1 61440 3050 /tmp/cert.pem /tmp/key.pem        # TLS 1.3
/tmp/tls_cost 2 61440 3050 /tmp/cert.pem /tmp/key.pem        # TLS 1.3 + kTLS transmit
```

Echo round trips over loopback with `TCP_NODELAY`, first 50 iterations discarded as warm-up. Sizes
are taken from the wire protocol rather than round numbers: **5 B** is a `PING`, **60 B** is one
pushed subscription row, **60 kB** is a `MINSERT` of a thousand levels or a `SELECT` response of
about a thousand rows.

Interleave the modes rather than running each to completion — the same discipline as every other
benchmark here, because a machine that warms up or throttles biases whichever mode ran second.

Eight rounds, i3-7100U:

| payload | plaintext | TLS 1.3 (OpenSSL) | TLS 1.3 + kTLS TX |
|---|---|---|---|
| 5 B | 31.94 µs (cv 2.7%) | 52.84 µs — **1.68×** (1.56–1.73) | 56.92 µs — 1.77× |
| 60 kB | 59.84 µs (cv 4.7%) | 230.28 µs — **3.70×** (3.64–4.20) | 265.48 µs — 4.38× |

Ratios are per-round medians with the range, not a ratio of means.

**What this measures and what it does not.** Loopback is the record layer's CPU cost with no NIC in
the way. kTLS exists to avoid a copy and to hand encryption to hardware that can do it, and neither
applies here — so "kTLS measured 1.08× and 1.15× slower" is evidence against using it *on this
path*, not evidence that kTLS is slow. That is the part of the conclusion that expires, and it
expires the day a NIC with TLS offload is in the picture.
