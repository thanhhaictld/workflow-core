# Builds from a git archive supplied as the Docker context. No checkout mutation.
FROM mcr.microsoft.com/dotnet/sdk:10.0
WORKDIR /src
COPY . .
RUN dotnet build scaling/WorkflowCore.Scaling.slnx -c Release
ENTRYPOINT ["dotnet", "test", "scaling/tests/WorkflowCore.Scaling.PostgreSQL.Tests/WorkflowCore.Scaling.PostgreSQL.Tests.csproj", "-c", "Release", "--no-build"]
