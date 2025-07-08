// AutoDownloadRepository.java
package fr.kage.nexus3;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AutoDownloadRepository implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoDownloadRepository.class);

    private final String baseUrl;
    private Path downloadPath;
    private boolean authenticate;
    private String username;
    private String password;

    private RestTemplate restTemplate;
    private ExecutorService executorService;

    private AtomicLong assetProcessed = new AtomicLong();
    private AtomicLong assetFound = new AtomicLong();
    private AtomicLong repositoriesProcessed = new AtomicLong();

    public AutoDownloadRepository(String baseUrl, String downloadPath, boolean authenticate, String username, String password) {
        this.baseUrl = requireNonNull(baseUrl);
        this.downloadPath = downloadPath == null ? null : Paths.get(downloadPath);
        this.authenticate = authenticate;
        this.username = username;
        this.password = password;
    }

    public void start() {
        try {
            if (downloadPath == null)
                downloadPath = Files.createTempDirectory("nexus3-auto");
            else if (!downloadPath.toFile().exists())
                Files.createDirectories(downloadPath);
            else if (!downloadPath.toFile().isDirectory() || !downloadPath.toFile().canWrite())
                throw new IOException("Not a writable directory: " + downloadPath);

            LOGGER.info("Starting automated download of all Nexus 3 repositories to {}", downloadPath);
            executorService = Executors.newFixedThreadPool(20);
            
            if (authenticate) {
                LOGGER.info("Configuring authentication for Nexus 3 repository discovery");
                org.springframework.boot.web.client.RestTemplateBuilder restTemplateBuilder = 
                    new org.springframework.boot.web.client.RestTemplateBuilder();
                restTemplate = restTemplateBuilder.basicAuthentication(username, password).build();
                
                // Set auth for Java to download individual assets using url.openStream();
                java.net.Authenticator.setDefault(new java.net.Authenticator() {
                    protected java.net.PasswordAuthentication getPasswordAuthentication() {
                        return new java.net.PasswordAuthentication(username, password.toCharArray());
                    }
                });
            } else {
                restTemplate = new RestTemplate();
            }

            executorService.submit(this);
            executorService.awaitTermination(2, TimeUnit.DAYS);

            LOGGER.info("Download completed! Processed {} repositories, found {} assets, downloaded {} latest versions", 
                       repositoriesProcessed.get(), assetFound.get(), assetProcessed.get());
        }
        catch (IOException e) {
            LOGGER.error("Unable to create/use directory for local data: " + downloadPath, e);
        }
        catch (InterruptedException e) {
            LOGGER.warn("Download interrupted");
        }
    }

    @Override
    public void run() {
        checkState(executorService != null, "Executor not initialized");
        
        try {
            List<Repository> repositories = discoverRepositories();
            LOGGER.info("Found {} repositories to process", repositories.size());
            
            for (Repository repo : repositories) {
                // Only process repositories that are online and of types that contain actual artifacts
                if (repo.isOnline() && ("hosted".equals(repo.getType()) || "proxy".equals(repo.getType()))) {
                    LOGGER.info("Processing repository: {} (format: {}, type: {})", repo.getName(), repo.getFormat(), repo.getType());
                    executorService.submit(new ProcessRepositoryTask(repo));
                } else {
                    LOGGER.info("Skipping repository {} - online: {}, type: {}", repo.getName(), repo.isOnline(), repo.getType());
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to discover repositories", e);
        }
    }

    private List<Repository> discoverRepositories() {
        try {
            UriComponentsBuilder uri = UriComponentsBuilder.fromHttpUrl(baseUrl)
                    .pathSegment("service", "rest", "v1", "repositories");

            LOGGER.info("Discovering repositories from: {}", uri.build().toUriString());
            ResponseEntity<Repository[]> response = restTemplate.getForEntity(uri.build().toUri(), Repository[].class);
            
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Repository[] repositories = response.getBody();
                LOGGER.info("Successfully discovered {} repositories", repositories.length);
                
                // Log repository details for debugging
                for (Repository repo : repositories) {
                    LOGGER.debug("Found repository: name={}, format={}, type={}, online={}", 
                               repo.getName(), repo.getFormat(), repo.getType(), repo.isOnline());
                }
                
                return Arrays.asList(repositories);
            } else {
                LOGGER.error("Failed to retrieve repositories. HTTP Status: {}", response.getStatusCode());
                return Collections.emptyList();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to retrieve repository list from Nexus API", e);
            LOGGER.error("Please verify:");
            LOGGER.error("1. Nexus URL is correct: {}", baseUrl);
            LOGGER.error("2. Nexus instance is accessible");
            LOGGER.error("3. Authentication credentials are correct (if required)");
            LOGGER.error("4. User has sufficient permissions to access repositories");
            return Collections.emptyList();
        }
    }

    void notifyProgress() {
        LOGGER.info("Progress: {} repos processed, {} assets found, {} latest versions downloaded", 
                   repositoriesProcessed.get(), assetFound.get(), assetProcessed.get());
    }

    private class ProcessRepositoryTask implements Runnable {
        private final Repository repository;

        public ProcessRepositoryTask(Repository repository) {
            this.repository = repository;
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Starting processing of repository: {}", repository.getName());
                
                // Collect all assets from this repository
                List<Item> allAssets = collectAllAssets(repository.getName());
                
                // Filter to get only the latest version of each artifact
                List<Item> latestAssets = filterLatestVersions(allAssets);
                
                LOGGER.info("Repository {}: Found {} total assets, {} unique latest versions", 
                           repository.getName(), allAssets.size(), latestAssets.size());
                
                // Download the latest versions
                for (Item item : latestAssets) {
                    executorService.submit(new DownloadItemTask(item, repository.getName()));
                }
                
                repositoriesProcessed.incrementAndGet();
                
            } catch (Exception e) {
                LOGGER.error("Failed to process repository: " + repository.getName(), e);
            }
        }

        private List<Item> collectAllAssets(String repositoryId) {
            List<Item> allAssets = new ArrayList<>();
            String continuationToken = null;
            int pageCount = 0;

            LOGGER.info("Collecting assets from repository: {}", repositoryId);

            do {
                try {
                    pageCount++;
                    UriComponentsBuilder getAssets = UriComponentsBuilder.fromHttpUrl(baseUrl)
                            .pathSegment("service", "rest", "v1", "assets")
                            .queryParam("repository", repositoryId);
                    
                    if (continuationToken != null) {
                        getAssets = getAssets.queryParam("continuationToken", continuationToken);
                    }

                    LOGGER.debug("Fetching assets page {} for repository {}", pageCount, repositoryId);
                    ResponseEntity<Assets> assetsEntity = restTemplate.getForEntity(getAssets.build().toUri(), Assets.class);
                    
                    if (!assetsEntity.getStatusCode().is2xxSuccessful()) {
                        LOGGER.error("Failed to retrieve assets for repository: {} - HTTP Status: {}", 
                                   repositoryId, assetsEntity.getStatusCode());
                        break;
                    }
                    
                    Assets assets = assetsEntity.getBody();
                    
                    if (assets != null && assets.getItems() != null) {
                        int pageAssets = assets.getItems().size();
                        allAssets.addAll(assets.getItems());
                        assetFound.addAndGet(pageAssets);
                        continuationToken = assets.getContinuationToken();
                        
                        LOGGER.debug("Page {}: Found {} assets, continuation token: {}", 
                                   pageCount, pageAssets, continuationToken != null ? "present" : "none");
                        
                        if (pageCount % 10 == 0) {
                            LOGGER.info("Repository {}: Processed {} pages, {} total assets so far", 
                                      repositoryId, pageCount, allAssets.size());
                        }
                    } else {
                        LOGGER.warn("Empty or null response for repository: {}", repositoryId);
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to retrieve assets for repository: " + repositoryId + " on page " + pageCount, e);
                    break;
                }
            } while (continuationToken != null);

            LOGGER.info("Repository {}: Collected {} total assets across {} pages", repositoryId, allAssets.size(), pageCount);
            return allAssets;
        }

        private List<Item> filterLatestVersions(List<Item> allAssets) {
            // First filter out metadata and non-primary artifacts
            List<Item> primaryAssets = allAssets.stream()
                    .filter(this::isPrimaryArtifact)
                    .collect(Collectors.toList());

            LOGGER.info("Filtered {} primary artifacts from {} total assets", primaryAssets.size(), allAssets.size());

            // Group assets by their artifact key (name without version)
            Map<String, List<Item>> groupedAssets = primaryAssets.stream()
                    .collect(Collectors.groupingBy(Item::getArtifactKey));

            List<Item> latestAssets = new ArrayList<>();

            for (Map.Entry<String, List<Item>> entry : groupedAssets.entrySet()) {
                List<Item> versions = entry.getValue();
                
                // Find the latest version by lastUpdated timestamp (most reliable)
                Item latest = versions.stream()
                        .max(Comparator.comparingLong(Item::getLastUpdated))
                        .orElse(null);

                if (latest != null) {
                    latestAssets.add(latest);
                    LOGGER.debug("Selected latest version for {}: {} (updated: {})", 
                               entry.getKey(), latest.getVersion(), latest.getLastUpdated());
                }
            }

            return latestAssets;
        }

        private boolean isPrimaryArtifact(Item item) {
            String path = item.getPath();
            if (path == null) return false;

            // Skip common metadata and checksum files
            String lowerPath = path.toLowerCase();
            
            // Maven-specific exclusions
            if ("maven2".equals(item.getFormat())) {
                if (lowerPath.endsWith(".md5") || lowerPath.endsWith(".sha1") || 
                    lowerPath.endsWith(".asc") || lowerPath.endsWith(".asc.md5") || 
                    lowerPath.endsWith(".asc.sha1") || lowerPath.contains("maven-metadata") ||
                    lowerPath.endsWith(".pom.md5") || lowerPath.endsWith(".pom.sha1")) {
                    return false;
                }
            }
            
            // npm-specific exclusions
            if ("npm".equals(item.getFormat())) {
                if (lowerPath.endsWith(".md5") || lowerPath.endsWith(".sha1") ||
                    path.contains("/-/")) { // npm metadata paths
                    return false;
                }
            }
            
            // NuGet-specific exclusions
            if ("nuget".equals(item.getFormat())) {
                if (lowerPath.endsWith(".md5") || lowerPath.endsWith(".sha1") ||
                    lowerPath.endsWith(".nuspec")) {
                    return false;
                }
            }
            
            // Docker-specific exclusions
            if ("docker".equals(item.getFormat())) {
                if (path.contains("/manifests/") || path.contains("/blobs/")) {
                    return false; // Skip manifest and blob files, keep only layer tars
                }
            }

            return true;
        }
    }

    private class DownloadItemTask implements Runnable {
        private final Item item;
        private final String repositoryName;

        public DownloadItemTask(Item item, String repositoryName) {
            this.item = item;
            this.repositoryName = repositoryName;
        }

        @Override
        public void run() {
            LOGGER.debug("Downloading latest asset from {}: {}", repositoryName, item.getPath());

            try {
                // Create repository-specific subdirectory
                Path repoPath = downloadPath.resolve(repositoryName);
                Path assetPath = repoPath.resolve(item.getPath());
                Files.createDirectories(assetPath.getParent());

                final URI downloadUri = URI.create(item.getDownloadUrl());
                int tryCount = 1;
                boolean downloadSuccessful = false;

                while (tryCount <= 3 && !downloadSuccessful) {
                    try (InputStream assetStream = downloadUri.toURL().openStream()) {
                        Files.copy(assetStream, assetPath);
                        
                        // Verify checksum if available
                        if (item.getChecksum() != null && item.getChecksum().getSha1() != null) {
                            final HashCode hash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                            if (Objects.equals(hash.toString(), item.getChecksum().getSha1())) {
                                downloadSuccessful = true;
                            } else {
                                LOGGER.warn("Checksum mismatch for {}, retrying (attempt {})", item.getPath(), tryCount);
                            }
                        } else {
                            downloadSuccessful = true; // No checksum to verify
                        }
                        
                        if (!downloadSuccessful) {
                            tryCount++;
                        }
                    }
                }

                if (downloadSuccessful) {
                    assetProcessed.incrementAndGet();
                    if (assetProcessed.get() % 10 == 0) { // Log progress every 10 downloads
                        notifyProgress();
                    }
                } else {
                    LOGGER.error("Failed to download asset after 3 attempts: {}", item.getDownloadUrl());
                }

            } catch (IOException e) {
                LOGGER.error("Failed to download asset <" + item.getDownloadUrl() + ">", e);
            }
        }
    }
}
