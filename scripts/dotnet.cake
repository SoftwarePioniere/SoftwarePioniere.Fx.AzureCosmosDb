#load "./utils.cake"
#load "./docker.cake"
#load "./gitversion.cake"
#addin "nuget:?package=Cake.FileHelpers"

#reference "System"

public class MyDotNet {

    private const string Prfx = "MyDotNet";

    private static ICakeContext _context;
    private static string _configuration;
    private static bool _isDryRun;
    private static string _vstsToken;
    private static string _nugetApiKey;

    public static void Init(ICakeContext context, string configuration, bool isDryRun, string vstsToken, string nugetApiKey) {
           _context = context;
           _configuration = configuration;
           _isDryRun = isDryRun;
           _vstsToken = vstsToken;
           _nugetApiKey = nugetApiKey;

           CreateNugetConfigTemp();
    }

    public static DotNetCoreVerbosity Verbosity { get ; set; } = DotNetCoreVerbosity.Minimal;

    private static void TestInit() {
        if (_context == null)
            throw new System.InvalidOperationException("Please Init context first");

         if (string.IsNullOrEmpty(_configuration))
            throw new System.InvalidOperationException("Please Init configuration first");
    }

    public static void CreateNugetConfigTemp() {
        TestInit();

        var buildFile = _context.File("./nuget.config.build");

        var fileName = "./nuget.config.build.tmp";
        var tmpFile = _context.File(fileName);

        if ( !_context.FileExists(buildFile)  ) {
            // if there is no build file copy default nuget.config
            var configFile = _context.File("./nuget.config");

            if ( !_context.FileExists(tmpFile)  )
            {
                _context.Verbose($"copy {configFile} to {tmpFile}");
                _context.CopyFile( configFile, tmpFile );
            }
        }
        else {

            if ( !_context.FileExists(tmpFile)  )
            {
                _context.Verbose($"copy {buildFile} to {tmpFile}");
                _context.CopyFile( buildFile, tmpFile );

                // _context.ReplaceTextInFiles(fileName, "{{VSTS_USER}}", EnvironmentVariable("VSTS_USER"));
                _context.ReplaceTextInFiles(fileName, "{{VSTS_TOKEN}}", _vstsToken);
                _context.ReplaceTextInFiles(fileName, "{{BUILD_CONFIG}}", _configuration);
            }
        }

    }

    public static string[] GetDockerTags(string image){
        TestInit();

        var tag = new [] {
            $"{image}:{MyGitVersion.GetVersion()}"
        };

        if (MyGit.GetBranch(_context).Contains("master") )
        {
            _context.Information("Master Branch. tagging latest");
            tag = new[] {
                tag[0],
                $"{image}:latest"
            };
        }

        return tag;
    }

    public static string[] GetDockerPackTags(string image){
        TestInit();

         var tag = new [] {
            $"{image}.packages:{MyGitVersion.GetVersion()}"
        };

        return tag;
    }

    public static string[] GetDockerTestRunnerTags(string image){
        TestInit();

         var tag = new [] {
            $"{image}.testrunner:{MyGitVersion.GetVersion()}"
        };

        return tag;
    }

      public static string[] GetDockerPushPackagesTags(string image){
        TestInit();

         var tag = new [] {
            $"{image}.push-packages:{MyGitVersion.GetVersion()}"
        };

        return tag;
    }

    public static void RestoreSolution(ConvertableFilePath solutionFile){
        TestInit();
        _context.Information("Restoring Packages...");

        var settings = new DotNetCoreRestoreSettings {
            ConfigFile = _context.File("./nuget.config"),
            Verbosity = Verbosity
        };

        if (!_isDryRun) {
             _context.DotNetCoreRestore(solutionFile.Path.FullPath, settings);
        } else {
             _context.Verbose("Dry Run, skipping DotNetCoreRestore");
        }

    }

    public static void BuildSolution(ConvertableFilePath solutionFile){
        TestInit();

        _context.Information("Building Solution...");

        var settings = new DotNetCoreBuildSettings {
            Configuration = _configuration,
            NoRestore = true,
            Verbosity = Verbosity,
            EnvironmentVariables = new Dictionary<string, string> {
                { "NuGetVersionV2", MyGitVersion.GetVersion() },
                { "AssemblySemVer", MyGitVersion.GetAssemblyVersion() }
            }
        };

        if (!_isDryRun) {
            _context.DotNetCoreBuild(solutionFile.Path.FullPath, settings);
        } else {
            _context.Verbose("Dry Run, skipping DotNetCoreBuild");
        }
    }

    public static void PackSolution(ConvertableFilePath solutionFile,ConvertableDirectoryPath artifactsDirectory){
        TestInit();

        _context.Information("Packing Solution...");

        var settings = new DotNetCorePackSettings {
            Configuration = _configuration,
            OutputDirectory =  artifactsDirectory + _context.Directory("packages"),
            NoRestore = true,
            NoBuild = true,
            IncludeSymbols = true,
            Verbosity = Verbosity,
            EnvironmentVariables = new Dictionary<string, string> {
                { "NuGetVersionV2", MyGitVersion.GetVersion() },
                { "AssemblySemVer", MyGitVersion.GetAssemblyVersion() }
            }
        };

        if (!_isDryRun) {
            _context.DotNetCorePack(solutionFile, settings);
        } else {
            _context.Verbose("Dry Run, skipping DotNetCorePack");
        }
    }

    public static void PushPackages(ConvertableDirectoryPath artifactsDirectory, DotNetCoreNuGetPushSettings settings){
        TestInit();

        _context.Information("Pushing Packages...");

        var pkgs = _context.GetFiles($"{artifactsDirectory.Path.FullPath}/packages/**/*.symbols.nupkg");

        foreach(var pk in pkgs)
        {
             _context.Information("Starting DotNetCoreNuGetPush on Package: {0}", pk.FullPath);

            if (!_isDryRun) {
                _context.DotNetCoreNuGetPush(pk.FullPath, settings);
            } else {
                _context.Verbose("Dry Run, skipping DotNetCoreNuGetPush");
            }
        }
    }

    public static void TestProjects(string filter){
        TestInit();

        _context.Information("Testing Projects...");

        var projects = _context.GetFiles(filter);
        foreach(var project in projects)
        {
            _context.Information($"Testing Project: {project.FullPath}");

            var settings = new DotNetCoreTestSettings
            {
                Configuration = _configuration,
                Verbosity = Verbosity,
                EnvironmentVariables = new Dictionary<string, string> {
                    { "NuGetVersionV2", MyGitVersion.GetVersion() },
                    { "AssemblySemVer", MyGitVersion.GetAssemblyVersion() }
                },
                NoBuild = true,
                NoRestore = true
            };

            if (!_isDryRun) {
                _context.DotNetCoreTest(project.FullPath, settings);
            } else {
                _context.Verbose("Dry Run, skipping DotNetCoreTest");
            }
        }
    }

    public static void PublishProjects(ConvertableDirectoryPath artifactsDirectory, string filter) {
        TestInit();

        _context.Information("Publishing Projects...");

        var projects = _context.GetFiles(filter);
        foreach(var project in projects)
        {
            _context.Information($"Publishing Project: {project.FullPath}");
            var projName = project.GetFilenameWithoutExtension().ToString();
            var outputDir = artifactsDirectory + _context.Directory(projName);
            _context.Information($"OutputDirectory: {outputDir}");

            var settings = new DotNetCorePublishSettings
            {
                Configuration = _configuration,
                OutputDirectory = outputDir,
                Verbosity = Verbosity,
                NoRestore = true,
                EnvironmentVariables = new Dictionary<string, string> {
                    { "NuGetVersionV2", MyGitVersion.GetVersion() },
                    { "AssemblySemVer", MyGitVersion.GetAssemblyVersion() }
                }
            };

            if (!_isDryRun) {
                _context.DotNetCorePublish(project.FullPath, settings);
                _context.Zip(outputDir, artifactsDirectory + _context.File($"{projName}-{MyGitVersion.GetVersion()}.zip") );
            } else {
                _context.Verbose("Dry Run, skipping DotNetCorePublish");
            }
        }
    }

    public static void DockerBuild(string image) {
        TestInit();

        _context.Information("Building with Docker...");

        var settings = new DockerImageBuildSettings {
        // Rm = true,
        Pull = true,
        BuildArg = new [] {
            $"CONFIGURATION={_configuration}",
            $"NUGETVERSIONV2={MyGitVersion.GetVersion()}",
            $"ASSEMBLYSEMVER={MyGitVersion.GetAssemblyVersion()}",
        },
        Tag = GetDockerTags(image)
        };

        if (!_isDryRun) {
             _context.DockerBuild(settings, ".");
        } else {
             _context.Verbose("Dry Run, skipping DockerBuild");
        }
    }

    public static void DockerBuildProject(string image, string project) {
        TestInit();

        _context.Information($"Building Project with Docker... {project}");

        var settings = new DockerImageBuildSettings {
        // Rm = true,
        Pull = true,
        BuildArg = new [] {
            $"CONFIGURATION={_configuration}",
            $"NUGETVERSIONV2={MyGitVersion.GetVersion()}",
            $"ASSEMBLYSEMVER={MyGitVersion.GetAssemblyVersion()}",
            $"PROJECT={project}",
            $"PROJECTDLL={project}.dll",
        },
        Tag = GetDockerTags(image)
        };

        if (!_isDryRun) {
             _context.DockerBuild(settings, ".");
        } else {
             _context.Verbose("Dry Run, skipping DockerBuild");
        }
    }

    public static void DockerTestProject(string image, string project, ConvertableDirectoryPath artifactsDirectory, string[] env = null) {
        TestInit();

        _context.Information($"Building Test Runner Images with Docker...");

        var testResultBaseDirectory = artifactsDirectory + _context.Directory("TestResults");
        var testResultDirectory = testResultBaseDirectory + _context.Directory(project);

        if  ( !_context.DirectoryExists(testResultBaseDirectory)) {
            _context.CreateDirectory(testResultBaseDirectory);
        }

        _context.CleanDirectories(new DirectoryPath[] { testResultDirectory });

        {
            var settings = new DockerImageBuildSettings {
            // Rm = true,
            Pull = true,
            BuildArg = new [] {
                $"CONFIGURATION={_configuration}",
                $"NUGETVERSIONV2={MyGitVersion.GetVersion()}",
                $"ASSEMBLYSEMVER={MyGitVersion.GetAssemblyVersion()}",
                $"PROJECT={project}"
            },
            Tag = GetDockerTestRunnerTags(image),
            Target = "testrunner"
            };

            if (!_isDryRun) {
                _context.DockerBuild(settings, ".");
            } else {
                _context.Verbose("Dry Run, skipping DockerBuild");
            }
        }

        _context.Information($"Running Test with Docker...");

        {
              var settings = new DockerContainerRunSettings {
                    Rm = true,
                    Volume = new [] {
                        _context.MakeAbsolute(testResultDirectory) + ":/testresults"
                    },
                    Env = env
                };

                var tags = GetDockerTestRunnerTags(image);
                var tag = tags[0];

                if (!_isDryRun) {
                    // _context.DockerRun(settings, tag, "") ; //, "test", "--logger:trx", "--no-build", "-k", _nugetApiKey);
                    _context.DockerRun(settings, tag, "dotnet", "test", "--logger:trx", "--no-build", "--no-restore"
                                , "-r" , "/testresults", "-c", _configuration
                                , $"/p:NuGetVersionV2={MyGitVersion.GetVersion()}" , $"/p:AssemblySemVer={MyGitVersion.GetAssemblyVersion()}"
                              );
                } else {
                    _context.Verbose("Dry Run, skipping DockerRun");
                }

        }



        // settings.Tag = GetDockerPushPackagesTags(image);
        // settings.Target = "pushpackages";

        // if (!_isDryRun) {
        //      _context.DockerBuild(settings, ".");
        // } else {
        //     _context.Verbose("Dry Run, skipping DockerBuild");
        // }

    }

    public static void DockerPack(string image) {
        TestInit();

        _context.Information($"Building Pack with Docker...");

        var settings = new DockerImageBuildSettings {
        // Rm = true,
        Pull = true,
        BuildArg = new [] {
            $"CONFIGURATION={_configuration}",
            $"NUGETVERSIONV2={MyGitVersion.GetVersion()}",
            $"ASSEMBLYSEMVER={MyGitVersion.GetAssemblyVersion()}"
        },
        Tag = GetDockerPackTags(image),
        Target = "pack"
        };

        if (!_isDryRun) {
             _context.DockerBuild(settings, ".");
        } else {
            _context.Verbose("Dry Run, skipping DockerBuild");
        }



        // settings.Tag = GetDockerPushPackagesTags(image);
        // settings.Target = "pushpackages";

        // if (!_isDryRun) {
        //      _context.DockerBuild(settings, ".");
        // } else {
        //     _context.Verbose("Dry Run, skipping DockerBuild");
        // }

    }

    public static void DockerPushPackages(string image) {
        TestInit();

        _context.Information($"Pushing Packages with Docker...");

        var settings = new DockerContainerRunSettings {
            Rm = true
        };

        var tags = GetDockerPackTags(image);
        var tag = tags[0];

        if (!_isDryRun) {
            _context.DockerRun(settings, tag, "dotnet", "nuget", "push", "/proj/packages/*.nupkg", "-k", _nugetApiKey);
        } else {
            _context.Verbose("Dry Run, skipping DockerRun");
        }

    }

    public static void DockerPush(string image) {
        TestInit();

        _context.Information("Pushing Docker...");
        MyDocker.Login();

        var tags = GetDockerTags(image);

        foreach (var tag in tags)
        {
            _context.Information($"Pushin Image/Tag: {tag}");

            if (!_isDryRun) {
                _context.DockerPush(tag);
            } else {
                _context.Verbose("Dry Run, skipping DockerPush");
            }
        }
    }


}