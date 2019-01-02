FROM microsoft/dotnet:2-sdk AS restore
ARG CONFIGURATION=Release
WORKDIR /proj
COPY nuget.config.build.tmp ./nuget.config
COPY Directory.Build.* ./
COPY *.sln ./
COPY src/SoftwarePioniere.ReadModel.Services.AzureCosmosDb/*.csproj ./src/SoftwarePioniere.ReadModel.Services.AzureCosmosDb/
COPY test/SoftwarePioniere.ReadModel.Services.AzureCosmosDb.Tests/*.csproj ./test/SoftwarePioniere.ReadModel.Services.AzureCosmosDb.Tests/
RUN dotnet restore SoftwarePioniere.AzureCosmosDb.sln

FROM restore as src
COPY . .

FROM src AS buildsln
ARG CONFIGURATION=Release
ARG NUGETVERSIONV2=99.99.99
ARG ASSEMBLYSEMVER=99.99.99.99
WORKDIR /proj/src/
RUN dotnet build /proj/SoftwarePioniere.AzureCosmosDb.sln -c $CONFIGURATION --no-restore /p:NuGetVersionV2=$NUGETVERSIONV2 /p:AssemblySemVer=$ASSEMBLYSEMVER

FROM buildsln as testrunner
ARG PROJECT=SoftwarePioniere.ReadModel.Services.AzureCosmosDb.Tests
# ARG CONFIGURATION=Release
# ARG NUGETVERSIONV2=99.99.99
# ARG ASSEMBLYSEMVER=99.99.99.99
WORKDIR /proj/test/$PROJECT
# ENTRYPOINT ["dotnet", "test", "--logger:trx"]
# RUN dotnet test -c $CONFIGURATION --no-restore --no-build /p:NuGetVersionV2=$NUGETVERSIONV2 /p:AssemblySemVer=$ASSEMBLYSEMVER -r /testresults

FROM buildsln as pack
ARG CONFIGURATION=Release
ARG NUGETVERSIONV2=99.99.99
ARG ASSEMBLYSEMVER=99.99.99.99
RUN dotnet pack /proj/SoftwarePioniere.AzureCosmosDb.sln -c $CONFIGURATION --no-restore --no-build /p:NuGetVersionV2=$NUGETVERSIONV2 /p:AssemblySemVer=$ASSEMBLYSEMVER -o /proj/packages
WORKDIR /proj/packages/