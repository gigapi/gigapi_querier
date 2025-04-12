FROM golang:1.23 as builder
WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o gigapi_querier .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/gigapi_querier .
CMD ["./gigapi_querier"]
