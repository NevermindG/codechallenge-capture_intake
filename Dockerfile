FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/capture-api ./cmd/api && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o /out/downstream ./cmd/downstream

FROM alpine:3.22
RUN addgroup -S app && adduser -S -G app app && mkdir -p /var/lib/captures/images && chown -R app:app /var/lib/captures
USER app
COPY --from=build /out/capture-api /usr/local/bin/capture-api
COPY --from=build /out/downstream /usr/local/bin/downstream
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/capture-api"]
