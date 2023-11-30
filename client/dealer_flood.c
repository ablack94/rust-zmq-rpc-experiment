#include <zmq.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>

#include <inttypes.h>
#include <math.h>
#include <time.h>
#include <errno.h>

intmax_t now() {
    long ms;
    time_t s;
    struct timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);

    s = spec.tv_sec;
    ms = round(spec.tv_nsec / 1.0e6);
    if(ms > 999) {
        s++;
        ms = 0;
    }

    return ((intmax_t)s) * 1000 + ms;
}

int main()
{
    int rc;
    void *context = zmq_ctx_new();
    assert(context != NULL);
    void *sock = zmq_socket(context, ZMQ_DEALER);
    assert(sock != NULL);
    rc = zmq_connect(sock, "tcp://localhost:9999");
    assert(rc == 0);

    const char req[] = "1";

    int suffix = 0;

    while(1) {
    	int rc = zmq_send(sock, req, strlen(req), 0);
        if(rc < 0) {
            printf("bad rc=%d errno=%s\n", rc, strerror(errno));
            break;
        }
    }

    zmq_close(sock);
    zmq_ctx_destroy(context);
}
