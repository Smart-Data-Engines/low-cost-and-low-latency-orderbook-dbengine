// Does this OpenSSL negotiate kTLS on a real socket?
//
// Decisive for #30 part three: with kTLS the record layer runs in the kernel and the data path
// stays ordinary send()/recv() - so the io_uring transport keeps working unchanged after the
// handshake. Without it, that path needs a memory-BIO rewrite or a named refusal.
// Numbers, method and what they do not cover: benchmarks/tls/README.md
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/bio.h>
#include <openssl/x509v3.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

// Certificate and key come from the command line: hardcoding them made the README's
// instructions untrue on any machine but the one this was written on.
static const char *g_cert = NULL, *g_key = NULL;

static int listen_fd, port;

static void *server_thread(void *arg) {
    (void)arg;
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate_file(ctx, g_cert, SSL_FILETYPE_PEM);
    SSL_CTX_use_PrivateKey_file(ctx, g_key, SSL_FILETYPE_PEM);
    // kTLS needs a cipher the kernel implements and TLS 1.2/1.3 with AES-GCM.
    SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS);
    int fd = accept(listen_fd, NULL, NULL);
    SSL *ssl = SSL_new(ctx);
    SSL_set_fd(ssl, fd);
    if (SSL_accept(ssl) != 1) { printf("server: SSL_accept failed\n"); ERR_print_errors_fp(stdout); return NULL; }
    printf("server: cipher=%s version=%s\n", SSL_get_cipher(ssl), SSL_get_version(ssl));
    printf("server: BIO_get_ktls_send=%d BIO_get_ktls_recv=%d\n",
           BIO_get_ktls_send(SSL_get_wbio(ssl)), BIO_get_ktls_recv(SSL_get_rbio(ssl)));
    SSL_write(ssl, "hello", 5);
    SSL_shutdown(ssl); SSL_free(ssl); close(fd); SSL_CTX_free(ctx);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    g_cert = argv[1];
    g_key  = argv[2];

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    bind(listen_fd, (struct sockaddr*)&a, sizeof(a));
    socklen_t l = sizeof(a); getsockname(listen_fd, (struct sockaddr*)&a, &l);
    port = ntohs(a.sin_port);
    listen(listen_fd, 1);

    pthread_t t; pthread_create(&t, NULL, server_thread, NULL);

    SSL_CTX *ctx = SSL_CTX_new(TLS_client_method());
    SSL_CTX_set_options(ctx, SSL_OP_ENABLE_KTLS);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    connect(fd, (struct sockaddr*)&a, sizeof(a));
    SSL *ssl = SSL_new(ctx);
    SSL_set_fd(ssl, fd);
    if (SSL_connect(ssl) != 1) { printf("client: SSL_connect failed\n"); ERR_print_errors_fp(stdout); return 1; }
    printf("client: BIO_get_ktls_send=%d BIO_get_ktls_recv=%d\n",
           BIO_get_ktls_send(SSL_get_wbio(ssl)), BIO_get_ktls_recv(SSL_get_rbio(ssl)));
    char buf[16] = {0};
    int n = SSL_read(ssl, buf, sizeof(buf));
    printf("client: read %d bytes: %.*s\n", n, n > 0 ? n : 0, buf);
    pthread_join(t, NULL);
    return 0;
}
