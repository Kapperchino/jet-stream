# syntax=docker/dockerfile:1

FROM golang:1.20 as Builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o /app/jet

FROM ubuntu

WORKDIR /app

COPY --from=Builder /app/jet /app/jet

RUN apt update && \
    apt install -y wget

EXPOSE 8080
EXPOSE 8081/udp
EXPOSE 8081

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.13 && ARCH=`uname -m` && \
    wget -O /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64; \
    chmod +x /bin/grpc_health_probe

ENTRYPOINT ["./jet"]