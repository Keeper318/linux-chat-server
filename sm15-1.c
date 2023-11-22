#define _GNU_SOURCE
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <unistd.h>

typedef unsigned long Bitset;

enum {
    SOCKETS = 2,
    INITIAL_MAX_EVENTS = 1024,
    BITSET_BITS = sizeof(Bitset) * CHAR_BIT
};

struct Message {
    struct iovec data;
    struct Message *next;
    int num_clients;
    int from;
};

typedef struct {
    struct Message *message;
    size_t offset;
} Client;

struct MessageBuffer {
    struct Message *head;
    Client *clients;
    Bitset *clients_ready;
    int clients_capacity;
};

void sigterm_handler(__attribute__((unused)) int signum) {
    _exit(0);
}

struct MessageBuffer msgbuf_new(void) {
    struct MessageBuffer buf = {.head = calloc(1, sizeof(*buf.head)),
                                .clients_capacity = BITSET_BITS};
    buf.clients =
        reallocarray(NULL, buf.clients_capacity, sizeof(buf.clients[0]));
    buf.clients_ready = calloc(buf.clients_capacity / BITSET_BITS,
                               sizeof(buf.clients_ready[0]));
    return buf;
}

void set_ready(struct MessageBuffer *msgbuf, int fd, int value) {
    Bitset mask = ~((Bitset)-1 >> 1);
    div_t index = div(fd, BITSET_BITS);
    if (value) {
        msgbuf->clients_ready[index.quot] |= mask >> index.rem;
    } else {
        msgbuf->clients_ready[index.quot] &= ~(mask >> index.rem);
    }
}

void add_client(struct MessageBuffer *msgbuf, int fd) {
    int *capacity = &msgbuf->clients_capacity;
    if (fd >= *capacity) {
        int old_capacity = *capacity;
        for (; fd >= *capacity; *capacity *= 2) {
        }
        msgbuf->clients = reallocarray(msgbuf->clients, *capacity,
                                       sizeof(msgbuf->clients[0]));
        msgbuf->clients_ready =
            reallocarray(msgbuf->clients_ready, *capacity / BITSET_BITS,
                         sizeof(msgbuf->clients_ready[0]));
        memset(msgbuf->clients_ready + old_capacity / BITSET_BITS, 0,
               (*capacity - old_capacity) / CHAR_BIT);
    }
    msgbuf->clients[fd] = (Client){msgbuf->head, 0};
    set_ready(msgbuf, fd, 1);
    ++msgbuf->head->num_clients;
}

void move_client(struct MessageBuffer *msgbuf, int fd, size_t bytes) {
    Client *client = &msgbuf->clients[fd];
    struct Message *msg = client->message;
    for (struct Message *next; (next = msg->next); msg = next) {
        size_t remaining =
            msg->from == fd ? 0 : msg->data.iov_len - client->offset;
        if (bytes < remaining) {
            client->offset += bytes;
            break;
        }
        bytes -= remaining;
        *client = (Client){next, 0};
        if (--msg->num_clients == 0) {
            free(msg->data.iov_base);
            free(msg);
        }
    }
}

void remove_client(struct MessageBuffer *msgbuf, int fd) {
    move_client(msgbuf, fd, -1);
    set_ready(msgbuf, fd, 0);
    --msgbuf->head->num_clients;
}

ssize_t msgbuf_read(struct MessageBuffer *msgbuf, int fd) {
    size_t capacity = sysconf(_SC_PAGE_SIZE);
    void *data = malloc(capacity);
    ssize_t size = read(fd, data, capacity);
    if (size <= 0) {
        free(data);
        return size;
    }
    if ((size_t)size < capacity / 2) {
        data = realloc(data, size);
    } else {
        while ((size_t)size == capacity) {
            data = realloc(data, capacity *= 2);
            ssize_t read_result =
                read(fd, (char *)data + size, capacity - size);
            size += read_result < 0 ? 0 : read_result;
        }
    }
    int num_clients = msgbuf->head->num_clients;
    struct Message *new_head = calloc(1, sizeof(*msgbuf->head));
    new_head->num_clients = num_clients;
    *msgbuf->head = (struct Message){{data, size}, new_head, num_clients, fd};
    msgbuf->head = new_head;
    return size;
}

ssize_t msgbuf_write(struct MessageBuffer *msgbuf, int fd) {
    struct iovec *messages = NULL;
    set_ready(msgbuf, fd, 1);
    Client *client = &msgbuf->clients[fd];
    int num_messages = 0;
    for (struct Message *msg = client->message; msg->next; msg = msg->next) {
        num_messages += msg->from != fd;
    }
    ssize_t nbytes = 0;
    int index = 0;
    messages = calloc(num_messages, sizeof(messages[0]));
    for (struct Message *msg = client->message; msg->next; msg = msg->next) {
        // Клиенту не отправляются его же сообщения
        if (msg->from == fd) {
            continue;
        }
        messages[index] =
            msg == client->message
                ? (struct iovec){(char *)msg->data.iov_base + client->offset,
                                 msg->data.iov_len - client->offset}
                : msg->data;
        nbytes += messages[index++].iov_len;
    }

    ssize_t write_result =
        num_messages == 0 ? 0 : writev(fd, messages, num_messages);
    free(messages);
    move_client(msgbuf, fd, write_result);
    if (write_result < nbytes) {
        if (errno == EPIPE) {
            remove_client(msgbuf, fd);
            close(fd);
        }
        set_ready(msgbuf, fd, 0);
    }
    return write_result;
}

void broadcast(struct MessageBuffer *msgbuf) {
    Bitset mask = ~((Bitset)-1 >> 1);
    int sets = msgbuf->clients_capacity / BITSET_BITS;
    int clients = msgbuf->head->num_clients;
    for (int i = 0; clients && i < sets; ++i) {
        for (Bitset set = msgbuf->clients_ready[i]; set;) {
            int j = __builtin_clzl(set);
            msgbuf_write(msgbuf, i * BITSET_BITS + j);
            set &= ~(mask >> j);
            --clients;
        }
    }
}

int create_tcp_socket(char *service) {
    struct addrinfo *res = NULL;
    struct addrinfo hint = {
        .ai_family = AF_INET6,
        .ai_socktype = SOCK_STREAM,
        .ai_flags = AI_PASSIVE,
    };
    getaddrinfo(NULL, service, &hint, &res);
    int sock = -1;
    for (struct addrinfo *ai = res; ai; ai = ai->ai_next) {
        sock = socket(ai->ai_family, ai->ai_socktype | SOCK_NONBLOCK, 0);
        if (sock < 0) {
            continue;
        }
        int one = 1;
        setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        setsockopt(sock, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
        if (bind(sock, ai->ai_addr, ai->ai_addrlen) < 0) {
            close(sock);
            sock = -1;
            continue;
        }
        if (listen(sock, SOMAXCONN) < 0) {
            close(sock);
            sock = -1;
            continue;
        }
        break;
    }
    freeaddrinfo(res);
    return sock;
}

int create_unix_socket(char *filename) {
    unlink(filename);
    int sock = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
    struct sockaddr_un name;
    name.sun_family = AF_UNIX;
    strncpy(name.sun_path, filename, sizeof(name.sun_path) - 1);
    if (bind(sock, (const struct sockaddr *)&name, sizeof(name)) < 0 ||
        listen(sock, SOMAXCONN) < 0) {
        sock = -1;
    }
    return sock;
}

int main(__attribute__((unused)) int argc, char *argv[]) {
    signal(SIGPIPE, SIG_IGN);
    struct sigaction act = {.sa_handler = sigterm_handler};
    sigaction(SIGTERM, &act, NULL);

    char *tcp_port = argv[1];
    char *unix_socket_filename = argv[2];
    int sockets[SOCKETS];
    if ((sockets[0] = create_tcp_socket(tcp_port)) == -1 ||
        (sockets[1] = create_unix_socket(unix_socket_filename)) == -1) {
        return 1;
    }

    int epollfd = epoll_create1(0);
    int max_events = INITIAL_MAX_EVENTS;
    struct epoll_event *events = calloc(max_events, sizeof(events[0]));
    events[0].events = EPOLLIN | EPOLLET;
    for (int i = 0; i < SOCKETS; ++i) {
        events[0].data.fd = sockets[i];
        epoll_ctl(epollfd, EPOLL_CTL_ADD, sockets[i], &events[0]);
    }
    struct MessageBuffer msgbuf = msgbuf_new();
    for (int nfds; (nfds = epoll_wait(epollfd, events, max_events, -1)) > 0;) {
        struct epoll_event *end = events + nfds;
        int new_messages = 0;
        for (struct epoll_event *event = events; event < end; ++event) {
            // Сначала отдельно обработать новые подключения и EPOLLIN, чтобы
            // накопить все входящие сообщения
            int fd = event->data.fd;
            if (fd == sockets[0] || fd == sockets[1]) {
                int conn_sock;
                while ((conn_sock = accept4(fd, NULL, NULL, SOCK_NONBLOCK)) !=
                       -1) {
                    event->events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
                    event->data.fd = conn_sock;
                    epoll_ctl(epollfd, EPOLL_CTL_ADD, conn_sock, event);
                    add_client(&msgbuf, conn_sock);
                }
                event->events = 0;
            } else if (event->events & EPOLLIN) {
                msgbuf_read(&msgbuf, fd);
                new_messages = 1;
            }
        }
        for (struct epoll_event *event = events; event < end; ++event) {
            // Обрабатываем закрытые соединения, затем обрабатываем EPOLLOUT с
            // отправкой
            int client = event->data.fd;
            if (event->events & (EPOLLRDHUP | EPOLLERR | EPOLLHUP)) {
                remove_client(&msgbuf, client);
                close(client);
                continue;
            }
            if (event->events & EPOLLOUT) {
                msgbuf_write(&msgbuf, client);
            }
        }
        // Если пришли новые сообщения, то делаем рассылку по всем готовым
        // клиентам
        if (new_messages) {
            broadcast(&msgbuf);
        }
        if (nfds == max_events) {
            events = reallocarray(events, max_events *= 2, sizeof(events[0]));
        }
    }
    return 0;
}
