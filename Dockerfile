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

ENTRYPOINT ["./jet"]