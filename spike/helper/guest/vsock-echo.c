/*
 * Smoke-test stand-in for flow-connector-init: binds AF_VSOCK port 49092 and
 * echoes every byte back. Injected into the guest root as /vsock-echo, so it
 * must be static - the connector image supplies no libc we can rely on.
 *
 * It stays silent on stderr on purpose. The reactor reads a leading space byte
 * there as connector-init's readiness signal, and the smoke test asserts on
 * what the workload writes.
 */
#include <linux/vm_sockets.h>
#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

#define VSOCK_PORT 49092

int main(void)
{
    int listen_fd = socket(AF_VSOCK, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("vsock-echo: socket");
        return 1;
    }

    struct sockaddr_vm addr = {
        .svm_family = AF_VSOCK,
        .svm_cid = VMADDR_CID_ANY,
        .svm_port = VSOCK_PORT,
    };
    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("vsock-echo: bind");
        return 1;
    }
    if (listen(listen_fd, 4) < 0) {
        perror("vsock-echo: listen");
        return 1;
    }

    for (;;) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            perror("vsock-echo: accept");
            return 1;
        }

        char buf[4096];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) {
            for (ssize_t off = 0; off < n;) {
                ssize_t written = write(fd, buf + off, (size_t)(n - off));
                if (written <= 0) {
                    break;
                }
                off += written;
            }
        }
        close(fd);
    }
}
