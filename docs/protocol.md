# Wire protocol

The client port speaks a line protocol over TCP, or over TLS with `--tls-client`, and a node with
`--unix-socket` speaks the same protocol on that Unix socket, for clients on its host (`nc -U`). It is
what both shipped clients speak, and what `nc` speaks. This page covers what a client needs in order to read
it: how a command is sent, how each answer is framed, and what can arrive in between. The commands
themselves are in [Query language](query-language.md) and [Server and flags](cli.md).

The examples marked as checked below are exchanges with a running server, byte for byte.
`tests/integration/test_docs_transcripts.py` replays each of them against the server the tree builds
and fails on any difference:
- `C:` is one line the client sends;
- `S:` is one line of the answer;
- `S:` with nothing after it is an empty line;
- `<version>` is the server's version.

## Connecting

The server speaks first: a banner, one `OK` line and an empty line.

<!-- transcript: checked -->
```
S: OK ob_tcp_server v<version>
S:
C: PING
S: PONG
```

A client reads the banner to its empty line before it sends anything else. When the server runs
with `--auth-secret-file`, the session must authenticate next ([`AUTH`](cli.md#auth)); until then
it may send only `AUTH`, `PING` and `QUIT`.

## Commands

A command is one line ending in `\n`. Its words are separated by spaces, and a quoted name keeps
its spaces (`'BTC-USD'.'BINANCE'`). `MINSERT` is the one command that runs over several lines: its
first line says how many levels follow, and each level is a line of its own.

<!-- transcript: checked -->
```
S: OK ob_tcp_server v<version>
S:
C: INSERT BTC-USD BINANCE bid 6500000 150 3 1700000000000000000
S: OK
S:
C: MINSERT BTC-USD BINANCE ask 2 1700000000500000000
C: 6500100 80 2
C: 6500200 120 5
S: OK
S:
```

A line longer than 256 KiB is answered `ERR line too long`, and the connection is closed: what
follows in the stream cannot be told from the rest of that line.

## Answers

Every answer begins with one of a fixed set of words, and the word says where the answer ends:

| Begins with | Ends at | Answers |
|---|---|---|
| `OK` | the first empty line | every command that succeeds, except the three below; a body, if any, is between the two |
| `ERR ` | the end of its line | a refusal, of any command |
| `PONG` | the end of its line | `PING` |
| `PRIMARY`, `REPLICA`, `STANDALONE`, `MULTI_MASTER` | the end of its line | `ROLE` |

<!-- transcript: checked -->
```
S: OK ob_tcp_server v<version>
S:
C: INSERT BTC-USD BINANCE bid 6500000 150 3 1700000000000000000
S: OK
S:
C: FLUSH
S: OK
S:
C: SELECT * FROM 'BTC-USD'.'BINANCE' WHERE timestamp BETWEEN 0 AND 9999999999999999999
S: OK
S: timestamp_ns	price	quantity	order_count	side	level	sequence_number
S: 1700000000000000000	6500000	150	3	0	0	1
S:
C: ROLE
S: STANDALONE
```

A row answer is `OK`, a header line naming the columns and a line for each row, tab separated, then
the empty line. The columns of `SELECT *` and the meaning of each are in
[Query language](query-language.md#column-names).

**A refusal names what it refused.** It names a word that is not a command, or the token a command
has no place for together with what the command takes (#107, #222):

<!-- transcript: checked -->
```
S: OK ob_tcp_server v<version>
S:
C: HELLO
S: ERR unknown command
C: FLUSH now
S: ERR unexpected token 'now'; FLUSH takes: FLUSH
```

The token comes back as it was sent, so a byte of it that is not UTF-8 is in the answer too. A
client cuts an answer out of its buffer in bytes and decodes it afterwards (#237).

**Bytes that begin no answer are not an answer.** The Python client refuses them as they arrive,
rather than read on until its timeout (#237).

## Pushed rows

A connection with a subscription receives `PUSH` lines whenever a matching row is written. A pushed
row is one line, beginning `PUSH ` and the subscription's id. It can arrive **in front of the
answer to a command**, so a reader takes complete `PUSH` lines off the front of its buffer before it
matches an answer. The format, the subscription commands and what happens to a client that does not
keep up are in [Query language](query-language.md#over-the-wire).

## Pipelining

A client may send several commands before it reads their answers. The server answers them in the
order they were sent, and sends every answer to one read of the socket in one write (#146). A
pipelining reader counts answers by their frames above; "read the next line" reads half an `OK`
body. What it is worth, measured, is in [Server and flags](cli.md#pipelining).

## Ending the session

`QUIT` ends it: the server answers nothing and closes the connection, after every answer to a
command before it has been sent.

<!-- transcript: checked -->
```
S: OK ob_tcp_server v<version>
S:
C: PING
S: PONG
C: QUIT
```

The server closes a session on its own when:
- an answer cannot be sent, because the client stopped reading
  ([Large responses and slow clients](cli.md#large-responses-and-slow-clients));
- a line is too long;
- an `AUTH` fails;
- the node shuts down. It answers what it can first, within `--drain-timeout-ms`.

## Compression

`COMPRESS LZ4`, sent first in a session (after `AUTH`, if any), is answered `OK COMPRESS LZ4` and an
empty line. After that answer, both directions change framing. Every command and every answer is a
four-byte big-endian length followed by an LZ4 frame of that many bytes, which holds the same text
as above. The Python client negotiates it with `compress=True`.

## TLS

With `--tls-client` the port speaks TLS 1.3 and nothing else. The banner is the first application
data after the handshake, and everything on this page then runs inside the TLS stream. Both shipped
clients verify the server's certificate and its name. Certificates and names are covered in
[Operations](operations.md#turning-on-tls), and the two cluster surfaces right after it.

## Rings in shared memory

A session through the node's Unix socket (`--unix-socket`) may move its bytes to a pair of rings in
shared memory: the same protocol, without a system call a round trip when both ends poll (#52).
`RINGS` asks for them, after `AUTH` where the node asks for it, and nothing may follow it on the
socket before its answer:

```
C: RINGS 1048576
S: OK RINGS 1048576
S:
```

The size is each direction's, a power of two the node takes (`--ring-bytes`); `RINGS` alone takes the
node's. The answer arrives with three descriptors (`SCM_RIGHTS`): the rings' memory (a `memfd`), the
node's bell and the client's bell (two `eventfd`s). From there commands go into one ring and answers
and pushed rows come out of the other, byte for byte what the socket would have carried, and the
socket carries nothing: closing it ends the session. A refusal is an `ERR rings: ...` line on the
socket, and the session goes on there.

The memory, mapped `MAP_SHARED` at its full size and sealed against shrinking and growing:

| Offset | Size | What |
|---|---|---|
| 0 | 64 | `magic` (8 bytes, `OBRING1\0`), `version` (4, `1`), 4 reserved, `capacity` (8): little-endian |
| 64 | 256 | the commands' header: four 64-byte lines holding `tail`, `head` (64-bit), `consumer_asleep`, `producer_asleep` (32-bit) |
| 320 | 256 | the answers' header, the same |
| 576 | `capacity` | the commands' bytes |
| 576 + `capacity` | `capacity` | the answers' bytes |

A counter is the number of bytes ever written (`tail`, by the producer) or read (`head`, by the
consumer); a byte's place is the counter modulo `capacity`. The node writes the answers' `tail` and
the commands' `head`, the client the other two, and each side checks the other's against its own:
the node ends a session whose counters say more was read than written, or read went back.

Waking is by flag, so a side that polls is never rung:
- a producer publishes `tail` (release), then a sequentially consistent fence, then rings the
  consumer's bell only if `consumer_asleep` is set;
- a consumer about to sleep sets `consumer_asleep`, fences, and looks at `tail` again: bytes that
  arrived in between mean it does not sleep; after waking it clears the flag;
- the same with `producer_asleep` and `head` for room in a full ring.

The shipped C++ client does this with `ClientConfig::rings` ([Operations](operations.md)).
