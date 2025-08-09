FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build
WORKDIR /src
COPY . .
RUN dotnet restore
RUN dotnet publish -c Release -o /out

FROM mcr.microsoft.com/dotnet/aspnet:9.0-alpine
ENV ASPNETCORE_URLS=http://0.0.0.0:8080
WORKDIR /app
COPY --from=build /out ./
ENTRYPOINT ["./Rinha.Gateway"]