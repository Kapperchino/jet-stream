# syntax=docker/dockerfile:1

FROM golang:1.19-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o jet

VOLUME ["/jet"]


EXPOSE 8080
EXPOSE 8081/udp
EXPOSE 8081

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.15 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

ENTRYPOINT ["./jet"]