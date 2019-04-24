#addin "Cake.DocFx"
#tool "docfx.console"

// public static class MyDocFx {

    private const string Prfx = "MyDocFx";

    public static void BuildDocFx(this ICakeContext context, ConvertableDirectoryPath artifactsDirectory, string docfxFile) {

        context.Information($"PublishDocs {docfxFile}");

        {
            context.DocFxMetadata(docfxFile);
        }

        {

            var settings = new DocFxBuildSettings {
                 OutputPath = artifactsDirectory + context.Directory("docs")
            };

            context.DocFxBuild(docfxFile, settings);
        }
    }
// }