# Multi-arch NativeAOT build with automatic RID mapping.
# Local (Apple Silicon) builds ARM64; CI can target AMD64 via --platform.

# Runtime base
FROM --platform=$TARGETPLATFORM mcr.microsoft.com/dotnet/aspnet:9.0-alpine AS base
LABEL maintainer="Jonathan Peris"
ENV ASPNETCORE_URLS=http://0.0.0.0:8080 \
    DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1
WORKDIR /app
USER app

# Build
FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build

# NativeAOT toolchain on Alpine/musl
RUN apk add --no-cache clang build-base zlib-dev

ARG BUILD_CONFIGURATION=Release
ARG AOT=true
ARG TRIM=true
# Optional override; if not set we map TARGETPLATFORM -> RID
ARG RID
ARG TARGETPLATFORM

# Keep nuget cache in layer
ENV NUGET_PACKAGES=/root/.nuget/packages

WORKDIR /src

# Determine RID early and persist it (restore + publish must match)
RUN set -eux; \
    if [ -z "${RID:-}" ]; then \
      case "$TARGETPLATFORM" in \
        "linux/amd64") RID="linux-musl-x64" ;; \
        "linux/arm64") RID="linux-musl-arm64" ;; \
        *) echo "Unsupported TARGETPLATFORM: $TARGETPLATFORM" >&2; exit 1 ;; \
      esac; \
    fi; \
    echo "Using RuntimeIdentifier=$RID"; \
    echo "$RID" > /tmp/rid

# Restore with RID for correct assets graph (fixes NETSDK1047/1064)
# Copy only the project file first to maximize layer caching
COPY Rinha.Gateway.csproj ./
RUN dotnet restore "Rinha.Gateway.csproj" \
    -r $(cat /tmp/rid) \
    -p:Configuration=$BUILD_CONFIGURATION \
    -p:PublishAot=$AOT \
    -p:PublishTrimmed=$TRIM

# Copy remaining sources
COPY . .

# Publish (perform restore again to avoid rare partial-cache issues)
RUN dotnet publish "Rinha.Gateway.csproj" \
      -c $BUILD_CONFIGURATION \
      -p:PublishAot=$AOT \
      -p:PublishTrimmed=$TRIM \
      -p:TieredPGO=true \
      -p:PublishSingleFile=true \
      -p:SelfContained=true \
      -p:InvariantGlobalization=true \
      -p:IlcOptimizationPreference=Speed \
      -r $(cat /tmp/rid) \
      -o /app/publish

# Final image
FROM --platform=$TARGETPLATFORM base AS final
WORKDIR /app
EXPOSE 8080
COPY --from=build /app/publish ./
ENTRYPOINT ["./Rinha.Gateway"]