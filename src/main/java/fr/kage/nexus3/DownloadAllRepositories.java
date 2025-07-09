package fr.kage.nexus3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.RestClientException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class DownloadAllRepositories {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadAllRepositories.class);
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_SECONDS = 10;

    private final String url;
    private final String baseDownloadPath;
    private final boolean authenticate;
    private final String username;
    private final String password;
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Path statusFile;
    private final Set<String> excludedRepos;
    private final Set<String> completedRepos;
    private final Set<String> failedRepos;

    public DownloadAllRepositories(String url, String baseDownloadPath, boolean authenticate, String username, String password) {
        this.url = url;
        this.baseDownloadPath = baseDownloadPath;
        this.authenticate = authenticate;
        this.username = username;
        this.password = password;
        this.statusFile = Paths.get(baseDownloadPath).resolve(".nexus-export-all-status.json");
        this.excludedRepos = new HashSet<>(Arrays.asList(
            "maven-central", "maven-public", "nuget-hosted", "nuget.org-proxy"
        )); // Common repos to exclude
        this.completedRepos = new HashSet<>();
        this.failedRepos = new HashSet<>();
    }

    public void start() {
        Instant startTime = Instant.now();
        
        try {
            LOGGER.info("================= Multi-Repository Export Start =================");
            LOGGER.info("Nexus URL: {}", url);
            LOGGER.info("Base Download Path: {}", baseDownloadPath);
            LOGGER.info("Authentication: {}", authenticate ? "Enabled" : "Disabled");
            LOGGER.info("===============================================================");

            // Create base directory
            createBaseDirectory();
            
            // Load previous status if exists
            loadPreviousStatus();
            
            // Get repository list
            List<RepositoryInfo> repositories = getRepositoryList();
            
            if (repositories == null || repositories.isEmpty()) {
                LOGGER.warn("⚠ No repositories found or accessible.");
                return;
            }

            // Filter repositories
            List<RepositoryInfo> filteredRepos = filterRepositories(repositories);
            
            LOGGER.info("Found {} total repositories, {} will be exported", 
                       repositories.size(), filteredRepos.size());
            
            if (!completedRepos.isEmpty()) {
                LOGGER.info("Resuming export - {} repositories already completed", completedRepos.size());
            }
            
            if (!failedRepos.isEmpty()) {
                LOGGER.info("Previous failures detected for {} repositories - they will be retried", failedRepos.size());
            }

            // Export each repository
            exportRepositories(filteredRepos);
            
            Duration duration = Duration.between(startTime, Instant.now());
            LOGGER.info("================= Multi-Repository Export Complete =================");
            LOGGER.info("Total export time: {} minutes", duration.toMinutes());
            LOGGER.info("Successfully exported: {} repositories", completedRepos.size());
            LOGGER.info("Failed exports: {} repositories", failedRepos.size());
            
            if (!failedRepos.isEmpty()) {
                LOGGER.warn("Failed repositories: {}", String.join(", ", failedRepos));
            }
            
            // Clean up status file on complete success
            if (failedRepos.isEmpty()) {
                cleanupStatusFile();
            }
            
        } catch (Exception e) {
            LOGGER.error("Critical error during multi-repository export", e);
            saveStatus(); // Save status on critical error
            throw new RuntimeException("Multi-repository export failed", e);
        }
    }

    private void createBaseDirectory() throws IOException {
        Path basePath = Paths.get(baseDownloadPath);
        if (!Files.exists(basePath)) {
            LOGGER.info("Creating base download directory: {}", basePath);
            Files.createDirectories(basePath);
        }
        if (!Files.isDirectory(basePath) || !Files.isWritable(basePath)) {
            throw new IOException("Base download path is not a writable directory: " + basePath);
        }
    }

    private void loadPreviousStatus() {
        try {
            if (Files.exists(statusFile)) {
                LOGGER.info("Found previous export status, loading...");
                Map<String, Object> status = objectMapper.readValue(statusFile.toFile(), new TypeReference<Map<String, Object>>() {});
                
                List<String> completed = (List<String>) status.getOrDefault("completed", new ArrayList<>());
                List<String> failed = (List<String>) status.getOrDefault("failed", new ArrayList<>());
                
                completedRepos.addAll(completed);
                failedRepos.addAll(failed);
                
                LOGGER.info("Loaded previous status: {} completed, {} failed", completed.size(), failed.size());
            }
        } catch (Exception e) {
            LOGGER.warn("Could not load previous status, starting fresh", e);
        }
    }

    private void saveStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("completed", new ArrayList<>(completedRepos));
            status.put("failed", new ArrayList<>(failedRepos));
            status.put("lastUpdate", System.currentTimeMillis());
            
            objectMapper.writeValue(statusFile.toFile(), status);
            LOGGER.debug("Status saved: {} completed, {} failed", completedRepos.size(), failedRepos.size());
        } catch (Exception e) {
            LOGGER.error("Failed to save export status", e);
        }
    }

    private void cleanupStatusFile() {
        try {
            Files.deleteIfExists(statusFile);
            LOGGER.info("Cleaned up status file after successful completion");
        } catch (Exception e) {
            LOGGER.warn("Could not clean up status file", e);
        }
    }

    private List<RepositoryInfo> getRepositoryList() {
        RestTemplate restTemplate = createRestTemplate();
        String repoListUrl = url + "/service/rest/v1/repositories";

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                LOGGER.info("Fetching repository list (attempt {}/{})", attempt, MAX_RETRIES);
                
                ResponseEntity<List> response = restTemplate.exchange(
                    repoListUrl, HttpMethod.GET, null, List.class
                );

                List<Map<String, Object>> repoMaps = response.getBody();
                return repoMaps.stream()
                    .map(this::mapToRepositoryInfo)
                    .collect(Collectors.toList());

            } catch (RestClientException e) {
                LOGGER.warn("Failed to fetch repository list (attempt {}/{}): {}", 
                           attempt, MAX_RETRIES, e.getMessage());
                
                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(RETRY_DELAY_SECONDS * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying", ie);
                    }
                } else {
                    throw new RuntimeException("Failed to fetch repository list after " + MAX_RETRIES + " attempts", e);
                }
            }
        }
        return Collections.emptyList();
    }

    private RestTemplate createRestTemplate() {
        RestTemplateBuilder builder = new RestTemplateBuilder()
            .setConnectTimeout(Duration.ofSeconds(30))
            .setReadTimeout(Duration.ofSeconds(60));
            
        if (authenticate) {
            builder = builder.basicAuthentication(username, password);
        }
        
        return builder.build();
    }

    private RepositoryInfo mapToRepositoryInfo(Map<String, Object> repoMap) {
        return new RepositoryInfo(
            (String) repoMap.get("name"),
            (String) repoMap.get("format"),
            (String) repoMap.get("type"),
            (String) repoMap.get("url")
        );
    }

    private List<RepositoryInfo> filterRepositories(List<RepositoryInfo> repositories) {
        return repositories.stream()
            .filter(repo -> !excludedRepos.contains(repo.getName()))
            .filter(repo -> !"proxy".equals(repo.getType())) // Skip proxy repos by default
            .filter(repo -> !completedRepos.contains(repo.getName())) // Skip already completed
            .collect(Collectors.toList());
    }

    private void exportRepositories(List<RepositoryInfo> repositories) {
        int totalRepos = repositories.size();
        int currentRepo = 0;

        for (RepositoryInfo repo : repositories) {
            currentRepo++;
            
            LOGGER.info("================= Repository {}/{} =================", currentRepo, totalRepos);
            LOGGER.info("▶ Exporting repository: {} (format: {}, type: {})", 
                       repo.getName(), repo.getFormat(), repo.getType());
            
            boolean success = exportSingleRepository(repo);
            
            if (success) {
                completedRepos.add(repo.getName());
                failedRepos.remove(repo.getName()); // Remove from failed if it was there
                LOGGER.info("✅ Successfully exported repository: {}", repo.getName());
            } else {
                failedRepos.add(repo.getName());
                LOGGER.error("❌ Failed to export repository: {}", repo.getName());
            }
            
            // Save status after each repository
            saveStatus();
            
            // Small delay between repositories to reduce server load
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Export interrupted");
                break;
            }
        }
    }

    private boolean exportSingleRepository(RepositoryInfo repo) {
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                LOGGER.info("Attempting export of '{}' (attempt {}/{})", repo.getName(), attempt, MAX_RETRIES);
                
                new DownloadRepository(
                    url,
                    repo.getName(),
                    baseDownloadPath,
                    authenticate,
                    username,
                    password
                ).start();
                
                return true; // Success
                
            } catch (Exception e) {
                LOGGER.error("Export attempt {} failed for repository '{}': {}", 
                           attempt, repo.getName(), e.getMessage());
                
                if (attempt < MAX_RETRIES) {
                    int delaySeconds = RETRY_DELAY_SECONDS * attempt; // Incremental delay
                    LOGGER.info("Retrying repository '{}' in {} seconds...", repo.getName(), delaySeconds);
                    try {
                        Thread.sleep(delaySeconds * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                } else {
                    LOGGER.error("Max retry attempts exceeded for repository '{}'", repo.getName());
                }
            }
        }
        return false; // All attempts failed
    }

    // Inner class for repository information
    private static class RepositoryInfo {
        private final String name;
        private final String format;
        private final String type;
        private final String url;

        public RepositoryInfo(String name, String format, String type, String url) {
            this.name = name;
            this.format = format;
            this.type = type;
            this.url = url;
        }

        public String getName() { return name; }
        public String getFormat() { return format; }
        public String getType() { return type; }
        public String getUrl() { return url; }
    }

    // Method to add repositories to exclude list
    public void addExcludedRepository(String repoName) {
        excludedRepos.add(repoName);
    }

    // Method to remove repositories from exclude list
    public void removeExcludedRepository(String repoName) {
        excludedRepos.remove(repoName);
    }

    // Method to get current status
    public Map<String, Set<String>> getStatus() {
        Map<String, Set<String>> status = new HashMap<>();
        status.put("completed", new HashSet<>(completedRepos));
        status.put("failed", new HashSet<>(failedRepos));
        return status;
    }
}