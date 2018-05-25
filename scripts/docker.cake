#load "./utils.cake"
#load "./gitversion.cake"
#load "./git.cake"
#reference "System"

public class MyDocker {

    private const string Prfx = "MyDocker";

    private static ICakeContext _context;
    private static bool _isDryRun;
    private static string _registry;
    private static string _user; 
    private static string _password;

    public static void Init(ICakeContext context, bool isDryRun, string registry, string user, string password) {
        _context = context;        
        _isDryRun = isDryRun;
        _registry = registry;
        _user = user; 
        _password = password;

    }

    private static void TestInit() {
        if (_context == null)
            throw new System.InvalidOperationException("Please Init context first");
        
        if (string.IsNullOrEmpty(_user))
            throw new System.InvalidOperationException("Please Init user first");

        if (string.IsNullOrEmpty(_password))
            throw new System.InvalidOperationException("Please Init password first");
    }

    
    public static void Login() {
                
        _context.Information("Docker Login Docker..."); 

        var settings = new DockerRegistryLoginSettings {
            Username = _user,
            Password = _password
        };
                
        if (!_isDryRun) {
            _context.DockerLogin(settings, _registry);     
        } else {
            _context.Verbose("Dry Run, skipping DockerLogin");
        }
       
    }   
  
}

