#include <zmq.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>

#include <inttypes.h>
#include <math.h>
#include <time.h>

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
    void *context = zmq_ctx_new();
    void *sock = zmq_socket(context, ZMQ_REQ);
    assert(sock != NULL);
    int one = 1;
    size_t size_one = sizeof(one);
    int recv_timeout_ms = 1000;
    int sndhwm = 10000000;
    assert(0 == zmq_setsockopt(sock, ZMQ_REQ_RELAXED, (const void*)&one, sizeof(one)));
    assert(0 == zmq_setsockopt(sock, ZMQ_REQ_CORRELATE, (const void*)&one, sizeof(one)));
    assert(0 == zmq_setsockopt(sock, ZMQ_RCVTIMEO, (const void*)&recv_timeout_ms, sizeof(recv_timeout_ms)));
    assert(0 == zmq_setsockopt(sock, ZMQ_SNDHWM, (const void*)&sndhwm, sizeof(sndhwm)));
    zmq_connect(sock, "tcp://localhost:9999");

    const char req[] = "1";

    while(1)
    {
        intmax_t start = now();
        char buf[1024] = {};

        for(size_t idx=0; idx<1000;++idx)
        {
            if(-1 == zmq_send(sock, req, strlen(req), ZMQ_DONTWAIT))
            {
                switch(errno)
                {
                case EAGAIN:
                    //printf("recv\n");
                    zmq_recv(sock, &buf, 1024, 0);
                    break;
                default:
                    printf("Uhoh %s\n", strerror(errno));
                    abort();
                }
            }
        }

        intmax_t end = now();
        intmax_t delta = (end - start);
        printf("1000 packets in %ld ~%f ms\n", delta, delta / 1000.);
    }

    zmq_close(sock);
    zmq_ctx_destroy(context);
}
