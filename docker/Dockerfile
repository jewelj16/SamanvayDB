# Builds samanvaydb-server and kvctl into a small runtime image.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/samanvaydb-server ./cmd/samanvaydb-server && \
    CGO_ENABLED=0 go build -trimpath -o /out/kvctl ./cmd/kvctl

FROM alpine:3.20
RUN adduser -D -u 10001 samanvay && mkdir /data && chown samanvay /data
COPY --from=build /out/samanvaydb-server /out/kvctl /usr/local/bin/
USER samanvay
VOLUME ["/data"]
EXPOSE 7000 8080
ENTRYPOINT ["/usr/local/bin/samanvaydb-server"]
