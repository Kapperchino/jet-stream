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
EXPOSE 8082

ENTRYPOINT ["./jet"]