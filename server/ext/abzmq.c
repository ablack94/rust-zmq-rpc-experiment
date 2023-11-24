#include <zmq.h>
#include <assert.h>


struct MyContext {
    void* context;
    void* socket;
}

void* abzmq_init() {
    MyContext *ctx = malloc(sizeof(MyContext));
    ctx->context = zmq_ctx_new();
    void* socket = zmq_socket(ctx->context, ZMQ_ROUTER);
    zmq_bind(socket, "tcp://localhost:9999");
    ctx->socket = socket;
    return ctx;
}

void abzmq_close(void* context) {
    MyContext *ctx = (MyContext*)context;
    zmq_close(ctx->socket);
    zmq_ctx_destroy(ctx->context);
    free(ctx);
}

struct MyMessage {
    zmq_msg_t client_id;
    zmq_msg_t blank;
    zmq_msg_t payload;
}

void abzmq_recv(void* context, MyMessage* msg) {
    MyContext *ctx = (MyContext*)context;
    int rc;
    int64_t more;
    size_t more_size = sizeof more;
    rc = zmq_msg_init(&msg->client_id);
    assert(rc == 0);
    rc = zmq_recv(ctx->socket, &msg->client_id);
    assert(rc == 0);
    rc = zmq_getsockopt(ctx->socket, ZMQ_RCVMORE, &more, &more_size);
    assert(rc == 0);
}



