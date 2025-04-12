FROM golang:1.24 as builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o gigapi_querier .

FROM debian:12
WORKDIR /
COPY --from=builder /app/gigapi_querier .
CMD ["/gigapi_querier"]
