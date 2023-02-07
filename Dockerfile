# syntax=docker/dockerfile:1

FROM golang:1.19-alpine
ARG LEADER=false
ARG RAFT_ID

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN go build -o jet

VOLUME ["/jet"]

EXPOSE 8080

ENTRYPOINT ["./jet"]