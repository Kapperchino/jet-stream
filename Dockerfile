# syntax=docker/dockerfile:1

FROM golang:1.20-alpine as Builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o /app/jet

FROM alpine

WORKDIR /app

COPY --from=Builder /app/jet /app/jet

VOLUME ["/jet"]

EXPOSE 8080
EXPOSE 8081/udp
EXPOSE 8081

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.13 && ARCH=`uname -m` && \
    if [ "$ARCH" == "x86_64" ]; then \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    else \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-arm64;  \
    fi &&\
    chmod +x /bin/grpc_health_probe

ENTRYPOINT ["./jet"]