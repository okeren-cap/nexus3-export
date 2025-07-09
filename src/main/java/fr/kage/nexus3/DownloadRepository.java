// Bulletproof DownloadRepository with state persistence and resume capability

package fr.kage.nexus3;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.util.UriComponentsBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.*;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DownloadRepository implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadRepository.class);
    private static final int MAX_RETRIES = 5;
    private static final int BASE_RETRY_DELAY_SECONDS = 30;
    private static final int MAX_RETRY_DELAY_SECONDS = 300; // 5 minutes max
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 60;

    private final String url;
    private final String repositoryId;
    private Path downloadPath;
    private Path stateFile;
    private Path downloadedAssetsFile;

    private boolean authenticate;
    private String username;
    private String password;

    private RestTemplate restTemplate;
    private ExecutorService executorService;
    private ScheduledExecutorService healthCheckExecutor;

    private AtomicLong assetProcessed = new AtomicLong();
    private AtomicLong assetFound = new AtomicLong();
    private AtomicInteger activeTasks = new AtomicInteger();
    private AtomicLong lastProgressUpdate = new AtomicLong(System.currentTimeMillis());
    
    private Set<String> downloadedAssets = ConcurrentHashMap.newKeySet();
    private Set<String> continuationTokensProcessed = ConcurrentHashMap.newKeySet();
    private ObjectMapper objectMapper = new ObjectMapper();

    public DownloadRepository(String url, String repositoryId, String downloadPath, boolean authenticate, String username, String password) {
        this.url = requireNonNull(url);
        this.repositoryId = requireNonNull(repositoryId);
        this.downloadPath = downloadPath == null ? null : Paths.get(downloadPath).resolve(repositoryId);
        this.authenticate = authenticate;
        this.username = username;
        this.password = password;
    }

    public void start() {
        Instant startTime = Instant.now();
        try {
            LOGGER.info("================= Nexus 3 Export Start =================");
            LOGGER.info("Repository URL: {}", url);
            LOGGER.info("Target Repository: {}", repositoryId);
            LOGGER.info("Export Path: {}", downloadPath != null ? downloadPath : "(temporary directory will be created)");
            LOGGER.info("Authentication: {}", authenticate ? "Enabled" : "None");
            LOGGER.info("Username: {}", authenticate ? username : "(n/a)");
            LOGGER.info("========================================================");

            setupDirectories();
            loadPreviousState();
            setupRestTemplate();
            setupExecutors();
            
            LOGGER.info("Starting export with resume capability");
            LOGGER.info("Previously downloaded assets: {}", downloadedAssets.size());
            LOGGER.info("Previously processed continuation tokens: {}", continuationTokensProcessed.size());
            
            // Start health check
            startHealthCheck();
            
            // Submit initial task
            executorService.submit(new DownloadAssetsTask(null));

            // Wait for completion with regular state saves
            waitForCompletion();
            
            Duration duration = Duration.between(startTime, Instant.now());
            LOGGER.info("Export completed in {} seconds", duration.toSeconds());
            LOGGER.info("Final stats: {} assets processed, {} assets found", assetProcessed.get(), assetFound.get());
            LOGGER.info("================== Export Finished =====================");
            
            // Clean up state files on successful completion
            cleanupStateFiles();

        } catch (Exception e) {
            LOGGER.error("Error during repository download", e);
            saveState(); // Save state on error for resume
            throw new RuntimeException("Export failed", e);
        }
    }

    private void setupDirectories() throws IOException {
        if (downloadPath == null) {
            downloadPath = Files.createTempDirectory("nexus3").resolve(repositoryId);
            LOGGER.info("No path specified. Using temporary directory: {}", downloadPath);
        }
        if (!Files.exists(downloadPath)) {
            LOGGER.info("Creating download directory: {}", downloadPath);
            Files.createDirectories(downloadPath);
        }
        if (!Files.isDirectory(downloadPath) || !Files.isWritable(downloadPath)) {
            throw new IOException("Not a writable directory: " + downloadPath);
        }
        
        // Setup state files
        stateFile = downloadPath.resolve(".nexus-export-state.json");
        downloadedAssetsFile = downloadPath.resolve(".nexus-export-downloaded.json");
    }

    private void loadPreviousState() {
        try {
            if (Files.exists(stateFile)) {
                LOGGER.info("Found previous state file, loading...");
                Map<String, Object> state = objectMapper.readValue(stateFile.toFile(), new TypeReference<Map<String, Object>>() {});
                assetProcessed.set(((Number) state.getOrDefault("assetProcessed", 0)).longValue());
                assetFound.set(((Number) state.getOrDefault("assetFound", 0)).longValue());
                continuationTokensProcessed.addAll((List<String>) state.getOrDefault("continuationTokensProcessed", new ArrayList<>()));
                LOGGER.info("Loaded state: {} processed, {} found, {} tokens processed", 
                           assetProcessed.get(), assetFound.get(), continuationTokensProcessed.size());
            }
            
            if (Files.exists(downloadedAssetsFile)) {
                LOGGER.info("Found previous downloaded assets file, loading...");
                List<String> assets = objectMapper.readValue(downloadedAssetsFile.toFile(), new TypeReference<List<String>>() {});
                downloadedAssets.addAll(assets);
                LOGGER.info("Loaded {} previously downloaded assets", downloadedAssets.size());
            }
        } catch (Exception e) {
            LOGGER.warn("Could not load previous state, starting fresh", e);
        }
    }

    private void saveState() {
        try {
            Map<String, Object> state = new HashMap<>();
            state.put("assetProcessed", assetProcessed.get());
            state.put("assetFound", assetFound.get());
            state.put("continuationTokensProcessed", new ArrayList<>(continuationTokensProcessed));
            state.put("lastUpdate", System.currentTimeMillis());
            
            objectMapper.writeValue(stateFile.toFile(), state);
            objectMapper.writeValue(downloadedAssetsFile.toFile(), new ArrayList<>(downloadedAssets));
            
            LOGGER.debug("State saved: {} processed, {} found", assetProcessed.get(), assetFound.get());
        } catch (Exception e) {
            LOGGER.error("Failed to save state", e);
        }
    }

    private void cleanupStateFiles() {
        try {
            Files.deleteIfExists(stateFile);
            Files.deleteIfExists(downloadedAssetsFile);
            LOGGER.info("Cleaned up state files after successful completion");
        } catch (Exception e) {
            LOGGER.warn("Could not clean up state files", e);
        }
    }

    private void setupRestTemplate() {
        RestTemplateBuilder builder = new RestTemplateBuilder()
            .setConnectTimeout(Duration.ofSeconds(30))
            .setReadTimeout(Duration.ofSeconds(180)); // 3 minutes
            
        if (authenticate) {
            builder = builder.basicAuthentication(username, password);
            Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray());
                }
            });
        }
        
        restTemplate = builder.build();
    }

    private void setupExecutors() {
        executorService = Executors.newFixedThreadPool(3); // Very conservative thread count
        healthCheckExecutor = Executors.newScheduledThreadPool(1);
    }

    private void startHealthCheck() {
        healthCheckExecutor.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            long lastUpdate = lastProgressUpdate.get();
            
            if (now - lastUpdate > HEALTH_CHECK_INTERVAL_SECONDS * 1000) {
                LOGGER.info("Health check - Active tasks: {}, Processed: {}, Found: {}", 
                           activeTasks.get(), assetProcessed.get(), assetFound.get());
                saveState(); // Regular state save
            }
        }, HEALTH_CHECK_INTERVAL_SECONDS, HEALTH_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void waitForCompletion() throws InterruptedException {
        while (activeTasks.get() > 0) {
            LOGGER.debug("Waiting for {} active tasks to complete", activeTasks.get());
            Thread.sleep(5000);
            
            // Save state every 5 seconds during execution
            saveState();
        }
        
        LOGGER.info("All download tasks completed. Shutting down executors.");
        executorService.shutdown();
        healthCheckExecutor.shutdown();
        
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
            LOGGER.warn("Executor did not terminate gracefully, forcing shutdown");
            executorService.shutdownNow();
        }
        
        healthCheckExecutor.shutdownNow();
    }

    @Override
    public void run() {
        checkState(executorService != null, "Executor not initialized");
        executorService.submit(new DownloadAssetsTask(null));
    }

    void notifyProgress() {
        lastProgressUpdate.set(System.currentTimeMillis());
        LOGGER.info("Progress: Downloaded {} assets out of {} found ({}%)", 
                   assetProcessed.get(), assetFound.get(), 
                   assetFound.get() > 0 ? (assetProcessed.get() * 100 / assetFound.get()) : 0);
    }

    private class DownloadAssetsTask implements Runnable {
        private final String continuationToken;

        public DownloadAssetsTask(String continuationToken) {
            this.continuationToken = continuationToken;
            activeTasks.incrementAndGet();
        }

        @Override
        public void run() {
            try {
                // Skip if we've already processed this continuation token
                String tokenKey = continuationToken != null ? continuationToken : "initial";
                if (continuationTokensProcessed.contains(tokenKey)) {
                    LOGGER.info("Skipping already processed continuation token: {}", tokenKey);
                    return;
                }
                
                int retryCount = 0;
                while (retryCount < MAX_RETRIES) {
                    try {
                        UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(url)
                                .path("/service/rest/v1/assets")
                                .queryParam("repository", repositoryId);

                        if (continuationToken != null) {
                            uriBuilder.queryParam("continuationToken", continuationToken);
                        }

                        String uri = uriBuilder.build().toUriString();
                        LOGGER.info("Fetching assets batch {} (attempt {}/{})", 
                                   continuationToken != null ? "with continuation" : "initial", 
                                   retryCount + 1, MAX_RETRIES);
                        
                        ResponseEntity<Assets> response = restTemplate.getForEntity(uri, Assets.class);

                        if (response.getBody() != null && response.getBody().getItems() != null) {
                            int itemCount = response.getBody().getItems().size();
                            LOGGER.info("Retrieved {} assets in this batch", itemCount);
                            
                            for (Item item : response.getBody().getItems()) {
                                // Skip if already downloaded
                                if (!downloadedAssets.contains(item.getPath())) {
                                    executorService.submit(new DownloadItemTask(item));
                                    assetFound.incrementAndGet();
                                } else {
                                    LOGGER.debug("Skipping already downloaded asset: {}", item.getPath());
                                }
                            }

                            // Mark this token as processed
                            continuationTokensProcessed.add(tokenKey);
                            
                            notifyProgress();

                            if (response.getBody().getContinuationToken() != null) {
                                // Add delay and submit next batch
                                Thread.sleep(2000); // 2 second delay between batches
                                executorService.submit(new DownloadAssetsTask(response.getBody().getContinuationToken()));
                            }
                        }
                        
                        // Success - break out of retry loop
                        break;
                        
                    } catch (HttpServerErrorException.InternalServerError e) {
                        retryCount++;
                        if (e.getMessage().contains("Timed out reading query result")) {
                            int delaySeconds = Math.min(BASE_RETRY_DELAY_SECONDS * (int) Math.pow(2, retryCount - 1), MAX_RETRY_DELAY_SECONDS);
                            LOGGER.warn("Server timeout occurred (attempt {}/{}). Retrying in {} seconds...", 
                                       retryCount, MAX_RETRIES, delaySeconds);
                            
                            if (retryCount < MAX_RETRIES) {
                                Thread.sleep(delaySeconds * 1000);
                            } else {
                                LOGGER.error("Max retries exceeded for assets batch. This batch will be skipped but can be resumed later.", e);
                                saveState(); // Save state before giving up
                            }
                        } else {
                            LOGGER.error("Non-timeout server error occurred", e);
                            saveState();
                            break;
                        }
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error in assets batch processing", e);
                        saveState();
                        break;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Assets task interrupted");
            } finally {
                activeTasks.decrementAndGet();
            }
        }
    }

    private class DownloadItemTask implements Runnable {
        private Item item;

        public DownloadItemTask(Item item) {
            this.item = item;
            activeTasks.incrementAndGet();
        }

        @Override
        public void run() {
            try {
                // Skip if already downloaded
                if (downloadedAssets.contains(item.getPath())) {
                    LOGGER.debug("Asset already downloaded, skipping: {}", item.getPath());
                    return;
                }
                
                LOGGER.info("Downloading asset: {}", item.getPath());
                Path relativeItemPath = Paths.get(item.getPath()).normalize();
                if (relativeItemPath.isAbsolute()) {
                    LOGGER.warn("Asset path was absolute. Forcing to relative: {}", relativeItemPath);
                    relativeItemPath = Paths.get(".").resolve(item.getPath().substring(1)).normalize();
                }
                Path assetPath = downloadPath.resolve(relativeItemPath);
                
                // Check if file already exists with correct checksum
                if (Files.exists(assetPath)) {
                    try {
                        HashCode existingHash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                        if (Objects.equals(existingHash.toString(), item.getChecksum().getSha1())) {
                            LOGGER.info("File already exists with correct checksum: {}", item.getPath());
                            downloadedAssets.add(item.getPath());
                            assetProcessed.incrementAndGet();
                            return;
                        }
                    } catch (IOException e) {
                        LOGGER.debug("Error checking existing file, will re-download: {}", item.getPath());
                    }
                }
                
                Files.createDirectories(assetPath.getParent());
                URI downloadUri = URI.create(item.getDownloadUrl());
                
                boolean downloaded = false;
                for (int tryCount = 1; tryCount <= 3; tryCount++) {
                    try (InputStream assetStream = downloadUri.toURL().openStream()) {
                        Files.copy(assetStream, assetPath, StandardCopyOption.REPLACE_EXISTING);
                        HashCode hash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                        if (Objects.equals(hash.toString(), item.getChecksum().getSha1())) {
                            LOGGER.info("Successfully downloaded and verified: {}", item.getPath());
                            downloadedAssets.add(item.getPath());
                            downloaded = true;
                            break;
                        }
                        LOGGER.warn("Checksum mismatch, retrying download (attempt {}/3): {}", tryCount, item.getPath());
                    } catch (IOException e) {
                        LOGGER.warn("Download failed (attempt {}/3): {}", tryCount, item.getPath(), e);
                        if (tryCount == 3) {
                            throw e;
                        }
                        Thread.sleep(1000 * tryCount); // Incremental delay
                    }
                }
                
                if (downloaded) {
                    assetProcessed.incrementAndGet();
                    
                    if (assetProcessed.get() % 10 == 0) {
                        notifyProgress();
                        saveState(); // Save state every 10 downloads
                    }
                }
                
            } catch (Exception e) {
                LOGGER.error("Failed to download asset: {}", item.getPath(), e);
                saveState(); // Save state on error
            } finally {
                activeTasks.decrementAndGet();
            }
        }
    }
}