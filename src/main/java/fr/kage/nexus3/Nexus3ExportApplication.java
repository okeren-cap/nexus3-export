// Enhanced Nexus3ExportApplication.java
package fr.kage.nexus3;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Nexus3ExportApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(Nexus3ExportApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        if (args.length >= 2) {
            String url = args[0];
            String exportPath = args[1];
            
            if (args.length == 3 && "auto".equals(args[2])) {
                // New auto mode - exports all repositories with latest versions only
                Properties credentials = loadCredentials();
                boolean authenticate = Boolean.valueOf(credentials.getProperty("authenticate", "false"));
                String username = removeTrailingQuotes(credentials.getProperty("username"));
                String password = removeTrailingQuotes(credentials.getProperty("password"));
                
                new AutoDownloadRepository(url, exportPath, authenticate, username, password).start();
                return;
            } else if (args.length >= 3) {
                // Original mode - specific repository
                String repoId = args[1];
                String downloadPath = args[2];
                
                Properties credentials = loadCredentials();
                boolean authenticate = Boolean.valueOf(credentials.getProperty("authenticate", "false"));
                String username = removeTrailingQuotes(credentials.getProperty("username"));
                String password = removeTrailingQuotes(credentials.getProperty("password"));
                
                new DownloadRepository(url, repoId, downloadPath, authenticate, username, password).start();
                return;
            }
        }

        System.out.println("Usage:");
        System.out.println("  Auto mode (all repositories, latest versions only):");
        System.out.println("\tnexus3 http://url.to/nexus3 /path/to/export auto");
        System.out.println();
        System.out.println("  Manual mode (specific repository, all versions):");
        System.out.println("\tnexus3 http://url.to/nexus3 repositoryId /path/to/export");
        System.out.println();
        System.out.println("Note: Authentication credentials should be provided in credentials.properties file");
        System.exit(1);
    }

    private Properties loadCredentials() {
        Properties properties = new Properties();

        java.io.File credentialsFile = new java.io.File(java.nio.file.Paths.get("").toAbsolutePath().toFile(), "credentials.properties");

        if (!credentialsFile.exists()) {
            return properties;
        }

        try {
            properties.load(new java.io.FileInputStream(credentialsFile));
        } catch(java.io.IOException e) {
            System.err.println("Credentials file " + credentialsFile.getAbsolutePath() + " could not be found or is malformed!");
            System.exit(1);
        }

        return properties;
    }

    private String removeTrailingQuotes(String value) {
        if (value == null) return null;
        
        String result; 
        if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("\'") && value.endsWith("\'"))) {
            result = value.substring(1, value.length() - 1);
        } else {
            result = value;
        }

        return result;
    }
}