FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /app

# Copy and restore solutions/projects
COPY ma7-MQ.sln .
COPY src/Ma7MQ.Core/Ma7MQ.Core.csproj src/Ma7MQ.Core/
COPY src/Ma7MQ.Server/Ma7MQ.Server.csproj src/Ma7MQ.Server/
RUN dotnet restore

# Copy all source code and publish
COPY . .
RUN dotnet publish src/Ma7MQ.Server/Ma7MQ.Server.csproj -c Release -o out

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/out .
EXPOSE 8080
ENTRYPOINT ["dotnet", "Ma7MQ.Server.dll"]
