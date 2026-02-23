FROM registry.access.redhat.com/ubi9/ubi-minimal AS builder

RUN microdnf update -y && \
    microdnf install -y tar gzip make gcc

ENV VALKEY_VERSION=9.0.2

RUN curl -L "https://github.com/valkey-io/valkey/archive/refs/tags/${VALKEY_VERSION}.tar.gz" -o valkey.tar.gz && \
    tar xzf valkey.tar.gz && \
    cd valkey-${VALKEY_VERSION} && \
    make && \
    make PREFIX=/build install



FROM registry.access.redhat.com/ubi9/ubi-minimal

RUN echo "valkey:x:1000:1000:ValKey User:/etc/ValKey:/sbin/nologin" >> /etc/passwd && \
    echo "valkey:x:1000:" >> /etc/group

RUN mkdir -p /etc/ValKey /var/lib/valkey && \
    chown -R valkey:valkey /etc/ValKey /var/lib/valkey

COPY --from=builder /build/bin/ /usr/local/bin/

COPY --chown=valkey:valkey ValKey.conf /etc/ValKey/ValKey.conf

USER valkey

WORKDIR /var/lib/valkey

EXPOSE 6379

CMD ["valkey-server", "/etc/ValKey/ValKey.conf"]

