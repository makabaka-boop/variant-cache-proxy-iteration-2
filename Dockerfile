# syntax=docker/dockerfile:1

# 构建阶段：编译代理与可控源站两个静态二进制。
FROM golang:1.22-alpine AS build
WORKDIR /src

# 先拷贝模块描述以利用层缓存。
COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -o /out/proxy  ./cmd/proxy && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
    -o /out/origin ./cmd/origin

# 运行阶段：verify 服务还需要 Go 工具链跑测试，因此测试镜像单独定义。
FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates
COPY --from=build /out/proxy  /usr/local/bin/proxy
COPY --from=build /out/origin /usr/local/bin/origin
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/proxy"]

# verify 镜像：内置完整源码与 Go 工具链，执行 go test ./...。
FROM golang:1.22-alpine AS verify
WORKDIR /src
COPY . .
CMD ["go", "test", "-race", "-count=1", "-v", "./..."]
