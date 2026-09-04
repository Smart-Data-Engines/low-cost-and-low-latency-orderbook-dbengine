// Cost of TLS on a loopback socket at this engine's message sizes.
//
// Three configurations, same harness: plaintext send/recv, TLS 1.3 through OpenSSL, and TLS 1.3
// with kTLS transmit (which the probe showed this build negotiates). Sizes chosen from the wire
// protocol: 5 bytes is a PING, ~60 bytes is one pushed subscription row, 60 kB is a MINSERT of a
// thousand levels or a SELECT response of about a thousand rows.
#define _GNU_SOURCE
// Numbers, method and what they do not cover: benchmarks/tls/README.md
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// Certificate and key come from the command line: hardcoding them made the README's
// instructions untrue on any machine but the one this was written on.
static const char *g_cert = NULL, *g_key = NULL;

static int   g_listen_fd, g_mode, g_ktls_ok;
static size_t g_size;
static long  g_iters;
static char *g_buf;

static double now_s(void) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    return (double)t.tv_sec + (double)t.tv_nsec / 1e9;
}

static void set_ctx_opts(SSL_CTX *ctx) {
    if (g_mode == 2) SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS);
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
}

// Echo server: read `size` bytes, write them back.
static void *server_thread(void *arg) {
    (void)arg;
    int fd = accept(g_listen_fd, NULL, NULL);
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    SSL_CTX *ctx = NULL; SSL *ssl = NULL;
    if (g_mode > 0) {
        ctx = SSL_CTX_new(TLS_server_method());
        SSL_CTX_use_certificate_file(ctx, g_cert, SSL_FILETYPE_PEM);
        SSL_CTX_use_PrivateKey_file(ctx, g_key, SSL_FILETYPE_PEM);
        set_ctx_opts(ctx);
        ssl = SSL_new(ctx); SSL_set_fd(ssl, fd);
        if (SSL_accept(ssl) != 1) { printf("accept failed\n"); return NULL; }
        g_ktls_ok = BIO_get_ktls_send(SSL_get_wbio(ssl));
    }
    char *b = malloc(g_size);
    for (long i = 0; i < g_iters; ++i) {
        size_t got = 0;
        while (got < g_size) {
            int n = (g_mode > 0) ? SSL_read(ssl, b + got, (int)(g_size - got))
                                 : (int)read(fd, b + got, g_size - got);
            if (n <= 0) { free(b); return NULL; }
            got += (size_t)n;
        }
        size_t sent = 0;
        while (sent < g_size) {
            int n = (g_mode > 0) ? SSL_write(ssl, b + sent, (int)(g_size - sent))
                                 : (int)write(fd, b + sent, g_size - sent);
            if (n <= 0) { free(b); return NULL; }
            sent += (size_t)n;
        }
    }
    free(b);
    if (ssl) { SSL_shutdown(ssl); SSL_free(ssl); SSL_CTX_free(ctx); }
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 4 && argc != 6) {
        fprintf(stderr,
                "usage: %s <mode> <payload_bytes> <iterations> [cert.pem key.pem]\n"
                "  mode 0 = plaintext, 1 = TLS 1.3, 2 = TLS 1.3 with kTLS transmit\n"
                "  cert and key are required for modes 1 and 2\n", argv[0]);
        return 2;
    }
    g_mode  = atoi(argv[1]);            // 0 plain, 1 TLS1.3 openssl, 2 TLS1.3 + kTLS tx
    g_size  = (size_t)atol(argv[2]);
    g_iters = atol(argv[3]);
    if (g_mode > 0) {
        if (argc != 6) {
            fprintf(stderr, "modes 1 and 2 need a certificate and a key\n");
            return 2;
        }
        g_cert = argv[4];
        g_key  = argv[5];
    }

    g_listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(g_listen_fd, (struct sockaddr*)&a, sizeof(a));
    socklen_t l = sizeof(a); getsockname(g_listen_fd, (struct sockaddr*)&a, &l);
    listen(g_listen_fd, 1);

    pthread_t t; pthread_create(&t, NULL, server_thread, NULL);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    connect(fd, (struct sockaddr*)&a, sizeof(a));
    SSL_CTX *ctx = NULL; SSL *ssl = NULL;
    if (g_mode > 0) {
        ctx = SSL_CTX_new(TLS_client_method());
        set_ctx_opts(ctx);
        ssl = SSL_new(ctx); SSL_set_fd(ssl, fd);
        if (SSL_connect(ssl) != 1) { printf("connect failed\n"); return 1; }
    }

    g_buf = malloc(g_size);
    memset(g_buf, 'x', g_size);
    char *in = malloc(g_size);

    // Warm-up: the first round trip pays page faults and, in TLS mode, session setup.
    for (long i = 0; i < 50; ++i) {
        size_t s = 0;
        while (s < g_size) { int n = (g_mode>0)?SSL_write(ssl,g_buf+s,(int)(g_size-s)):(int)write(fd,g_buf+s,g_size-s); s += (size_t)n; }
        size_t g = 0;
        while (g < g_size) { int n = (g_mode>0)?SSL_read(ssl,in+g,(int)(g_size-g)):(int)read(fd,in+g,g_size-g); g += (size_t)n; }
    }

    const double t0 = now_s();
    for (long i = 50; i < g_iters; ++i) {
        size_t s = 0;
        while (s < g_size) { int n = (g_mode>0)?SSL_write(ssl,g_buf+s,(int)(g_size-s)):(int)write(fd,g_buf+s,g_size-s); s += (size_t)n; }
        size_t g = 0;
        while (g < g_size) { int n = (g_mode>0)?SSL_read(ssl,in+g,(int)(g_size-g)):(int)read(fd,in+g,g_size-g); g += (size_t)n; }
    }
    const double el = now_s() - t0;
    const long done = g_iters - 50;

    printf("mode=%d size=%zu rtt_us=%.2f MB/s=%.1f ktls_tx=%d\n",
           g_mode, g_size, el / (double)done * 1e6,
           (double)done * (double)g_size * 2.0 / el / 1e6, g_ktls_ok);
    pthread_join(t, NULL);
    return 0;
}
