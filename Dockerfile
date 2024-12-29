FROM eclipse-temurin:21-alpine

ARG VERSION=1.0-SNAPSHOT
COPY target/search-dump-${VERSION}-dist.tar.gz /search-dump-${VERSION}-dist.tar.gz
RUN tar -xzvf /search-dump-${VERSION}-dist.tar.gz && \
    ln -s /search-dump-${VERSION} /search-dump && \
    rm -f /search-dump-${VERSION}-dist.tar.gz && \
    mkdir /data
ENV VERSION=${VERSION}
WORKDIR /search-dump
CMD ["bin/search-dump.sh"]
