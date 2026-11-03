# One build recipe for the standalone Hub or DB-free remote worker.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
ARG PROJECT
RUN test -n "$PROJECT" && dotnet publish "$PROJECT" -c Release -o /out /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0
WORKDIR /app
COPY --from=build /out ./
USER app
ENTRYPOINT ["dotnet"]
