FROM --platform=$BUILDPLATFORM golang:1.22-bookworm AS build

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN go build ./...
RUN CGO_ENABLED=0 go build -o /out/orchestrator ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/orchestrator /app/orchestrator
EXPOSE 8080
ENTRYPOINT ["/app/orchestrator"]
