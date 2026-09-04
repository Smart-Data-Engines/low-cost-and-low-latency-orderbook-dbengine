// Which SSL_CTX modes the engine's send path needs, and why (#30 part three).
//
// Numbers, method and what they do not cover: benchmarks/tls/README.md
//
// `Session::flush_output()` writes from `send_buf_` and then does `erase(0, n)`, which moves the
// remaining bytes to a *different address*. OpenSSL requires a retried SSL_write to present the
// same buffer unless SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER is set, so this reproduces exactly that
// shape rather than a retry from an advanced offset - the two are different, and the second one is
// legal, which is why the first two versions of this probe found nothing.
#define _GNU_SOURCE
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int g_listen, g_modes;
static const char *g_cert, *g_key;

static void *srv(void *a) {
    (void)a;
    int fd = accept(g_listen, NULL, NULL);
    int rcv = 2048; setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));
    SSL_CTX *c = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate_file(c, g_cert, SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(c, g_key, SSL_FILETYPE_PEM);
    SSL *s = SSL_new(c); SSL_set_fd(s, fd);
    if (SSL_accept(s) != 1) { printf("server handshake failed\n"); return NULL; }
    sleep(5);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 4) { fprintf(stderr, "usage: %s <1 both|2 partial-only> <cert> <key>\n", argv[0]); return 2; }
    g_modes = atoi(argv[1]); g_cert = argv[2]; g_key = argv[3];

    g_listen = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(g_listen, (struct sockaddr*)&a, sizeof(a));
    socklen_t l = sizeof(a); getsockname(g_listen, (struct sockaddr*)&a, &l);
    listen(g_listen, 1);
    pthread_t t; pthread_create(&t, NULL, srv, NULL);

    SSL_CTX *c = SSL_CTX_new(TLS_client_method());
    if (g_modes == 1) SSL_CTX_set_mode(c, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    else              SSL_CTX_set_mode(c, SSL_MODE_ENABLE_PARTIAL_WRITE);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int snd = 4096; setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &snd, sizeof(snd));
    connect(fd, (struct sockaddr*)&a, sizeof(a));
    SSL *s = SSL_new(c); SSL_set_fd(s, fd);
    if (SSL_connect(s) != 1) { printf("client handshake failed\n"); return 1; }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);

    const size_t N = 2u * 1024u * 1024u;
    char *A = malloc(N), *B = malloc(N);      // two allocations: the retry uses the other one
    memset(A, 'x', N);
    size_t remaining = N;
    char *cur = A, *other = B;

    for (int i = 0; i < 100000; ++i) {
        ERR_clear_error();
        int n = SSL_write(s, cur, (int)remaining);
        if (n > 0) {
            // Accepted some. Move the remainder to the OTHER buffer, exactly as erase(0, n) moves
            // the remainder within one allocation - a different address for the same bytes.
            remaining -= (size_t)n;
            if (remaining == 0) { printf("modes=%d: whole buffer written (inconclusive)\n", g_modes); return 0; }
            memmove(other, cur + n, remaining);
            char *tmp = cur; cur = other; other = tmp;
            continue;
        }
        const int e = SSL_get_error(s, n);
        if (e != SSL_ERROR_WANT_WRITE && e != SSL_ERROR_WANT_READ) {
            printf("modes=%d: unexpected ssl_error=%d\n", g_modes, e);
            return 1;
        }
        // Blocked with a write pending. Now move the pending bytes to the other address and retry -
        // this is the case ACCEPT_MOVING_WRITE_BUFFER is about.
        usleep(150000);
        memmove(other, cur, remaining);
        char *tmp = cur; cur = other; other = tmp;
        ERR_clear_error();
        int n2 = SSL_write(s, cur, (int)remaining);
        if (n2 > 0) { printf("modes=%d: moved-address retry ACCEPTED (%d bytes)\n", g_modes, n2); return 0; }
        const int e2 = SSL_get_error(s, n2);
        unsigned long err = ERR_peek_error();
        char msg[256] = {0};
        if (err) ERR_error_string_n(err, msg, sizeof(msg));
        const char *name = e2 == SSL_ERROR_WANT_WRITE ? "WANT_WRITE"
                         : e2 == SSL_ERROR_WANT_READ  ? "WANT_READ"
                         : e2 == SSL_ERROR_SSL        ? "SSL — a real refusal" : "other";
        printf("modes=%d: moved-address retry -> %s%s%s\n", g_modes, name, err ? ": " : "", err ? msg : "");
        return 0;
    }
    printf("modes=%d: never blocked\n", g_modes);
    return 0;
}
