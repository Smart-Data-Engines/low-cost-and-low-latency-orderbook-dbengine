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

## `ssl_write_retry.c` — which `SSL_CTX` modes the send path needs

```bash
cc -O2 -o /tmp/ssl_write_retry benchmarks/tls/ssl_write_retry.c -lssl -lcrypto -lpthread
/tmp/ssl_write_retry 2 /tmp/cert.pem /tmp/key.pem   # SSL_MODE_ENABLE_PARTIAL_WRITE only
/tmp/ssl_write_retry 1 /tmp/cert.pem /tmp/key.pem   # both modes
```

`Session::flush_output()` writes from `send_buf_` and then does `erase(0, n)`, which moves the
remaining bytes to a **different address**. Measured, OpenSSL 3.0.13:

| modes | retrying the same bytes from a different address |
|---|---|
| `ENABLE_PARTIAL_WRITE` only | **`error:0A00007F:SSL routines::bad write retry`** |
| both | `WANT_WRITE`, an ordinary "come back later" |
| neither | the question does not arise: the first `WANT` lands at offset **0** |

That last row is a separate finding: **without `ENABLE_PARTIAL_WRITE`, `SSL_write` accepts nothing
at all** until it can take the whole buffer — measured at 4 MB, first `WANT` at offset 0.

**Corrected after implementing it, because this paragraph claimed too much.** That does *not* mean a
large response stalls: OpenSSL resumes its own pending write across retries, so the bytes arrive
either way — measured through a real `Session`, the content assertion passes with the mode removed.
What breaks is the *caller's* view. `send_buf_` never shrinks until the final call, so
`pending_output_bytes()` — the number `ob_pending_bytes` publishes, and the one an operator reads as
"this client is not draining" — stays pinned at the full response. Of the two modes, only
`ACCEPT_MOVING_WRITE_BUFFER` is required for correctness.

**And the hazard window is narrower than it looks.** `sent_total` advances only on a *fully*
accepted `SSL_write`, so with a socket send buffer below one TLS record (16 kB) every `WANT_WRITE`
arrives with `sent_total == 0`, the erase is skipped, and the retry presents the same address —
which is legal. The hazard needs a buffer that accepts at least one whole record and *then* blocks.
Three versions of the regression test used 4 kB and detected nothing; the fourth used 64 kB and the
mutation failed immediately.

**Worth knowing about this probe: its first two versions found nothing.** They retried from an
advanced offset in the same allocation, which presents the *same address* for the pending bytes and
is legal. Only moving those bytes elsewhere — what `erase(0, n)` does — produces `bad write retry`.
A probe that does not reproduce the shape says "no defect" in the same voice as a probe under which
there genuinely is none.

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
