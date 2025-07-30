// src/main/java/fr/kage/nexus3/DownloadRepository.java

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

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DownloadRepository implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadRepository.class);

    private final String url;
    private final String repositoryId;
    private Path downloadPath;

    private boolean authenticate;
    private String username;
    private String password;

    private RestTemplate restTemplate;
    private ExecutorService executorService;

    private AtomicLong assetProcessed = new AtomicLong();
    private AtomicLong assetFound = new AtomicLong();
    private AtomicInteger activeTasks = new AtomicInteger();
    private AtomicInteger pendingAssetDiscoveryTasks = new AtomicInteger();

    // Track discovered assets to prevent duplicates in hybrid mode
    private final Set<String> discoveredAssetIds = ConcurrentHashMap.newKeySet();

    private enum ApiMode {
        SEARCH_API,    // Current approach - fast but may miss some files
        ASSETS_API,    // Comprehensive listing - slower but gets everything
        HYBRID         // Use both APIs for maximum coverage
    }

    private ApiMode apiMode = ApiMode.HYBRID; // Default to hybrid for maximum coverage

    public DownloadRepository(String url, String repositoryId, String downloadPath, boolean authenticate, String username, String password) {
        this(url, repositoryId, downloadPath, authenticate, username, password, getApiModeFromSystemProperty());
    }

    public DownloadRepository(String url, String repositoryId, String downloadPath, boolean authenticate, String username, String password, ApiMode apiMode) {
        this.url = requireNonNull(url);
        this.repositoryId = requireNonNull(repositoryId);
        this.downloadPath = downloadPath == null ? null : Paths.get(downloadPath).resolve(repositoryId);
        this.authenticate = authenticate;
        this.username = username;
        this.password = password;
        this.apiMode = apiMode != null ? apiMode : ApiMode.HYBRID;
    }

    /**
     * Get API mode from system property. Allows users to override the default behavior.
     * 
     * Usage:
     * -Dnexus.export.api.mode=SEARCH_API    (Original fast mode, may miss some files)
     * -Dnexus.export.api.mode=ASSETS_API    (Comprehensive mode, slower)
     * -Dnexus.export.api.mode=HYBRID        (Default: Use both APIs for maximum coverage)
     */
    private static ApiMode getApiModeFromSystemProperty() {
        String property = System.getProperty("nexus.export.api.mode", "HYBRID").toUpperCase();
        try {
            return ApiMode.valueOf(property);
        } catch (IllegalArgumentException e) {
            LOGGER.warn("Invalid API mode '{}' specified in system property. Using default HYBRID mode. Valid options: SEARCH_API, ASSETS_API, HYBRID", property);
            return ApiMode.HYBRID;
        }
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

            LOGGER.info("Preparing download path");
            if (downloadPath == null) {
                downloadPath = Files.createTempDirectory("nexus3").resolve(repositoryId);
                LOGGER.info("No path specified. Using temporary directory: {}", downloadPath);
            }
            if (!Files.exists(downloadPath)) {
                LOGGER.info("Creating specified download directory: {}", downloadPath);
                Files.createDirectories(downloadPath);
            }
            if (!Files.isDirectory(downloadPath) || !Files.isWritable(downloadPath)) {
                throw new IOException("Not a writable directory: " + downloadPath);
            }

            LOGGER.info("Starting download of Nexus 3 repository '{}' into '{}' using {} mode", repositoryId, downloadPath, apiMode);
            executorService = Executors.newFixedThreadPool(10);

            if (authenticate) {
                LOGGER.info("Authentication enabled. Configuring credentials for REST and stream download.");
                restTemplate = new RestTemplateBuilder()
                        .basicAuthentication(username, password)
                        .setConnectTimeout(Duration.ofMinutes(2))
                        .setReadTimeout(Duration.ofMinutes(5))
                        .build();
                Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray());
                    }
                });
            } else {
                LOGGER.info("Authentication not required.");
                restTemplate = new RestTemplateBuilder()
                        .setConnectTimeout(Duration.ofMinutes(2))
                        .setReadTimeout(Duration.ofMinutes(5))
                        .build();
            }

            LOGGER.info("Submitting initial task to executor using {} for comprehensive file discovery", apiMode);
            pendingAssetDiscoveryTasks.incrementAndGet();
            
            switch (apiMode) {
                case SEARCH_API:
                    executorService.submit(new DownloadAssetsTask(null));
                    break;
                case ASSETS_API:
                    executorService.submit(new DownloadAssetsViaAssetsApiTask(null));
                    break;
                case HYBRID:
                    // Start with Assets API for comprehensive listing
                    executorService.submit(new DownloadAssetsViaAssetsApiTask(null));
                    // Also run Search API to catch any additional metadata
                    pendingAssetDiscoveryTasks.incrementAndGet();
                    executorService.submit(new DownloadAssetsTask(null));
                    break;
            }

            // Wait for all asset discovery and download tasks to complete
            while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                int activeTasks = this.activeTasks.get();
                int pendingDiscovery = pendingAssetDiscoveryTasks.get();
                
                LOGGER.debug("Status check - Active tasks: {}, Pending discovery: {}", activeTasks, pendingDiscovery);
                
                // Only shutdown when both asset discovery and download tasks are complete
                if (activeTasks == 0 && pendingDiscovery == 0) {
                    LOGGER.info("All asset discovery and download tasks completed. Shutting down executor.");
                    executorService.shutdown();
                } else if (activeTasks > 0 || pendingDiscovery > 0) {
                    LOGGER.info("Still processing - Active tasks: {}, Pending discovery: {}", activeTasks, pendingDiscovery);
                }
            }

            Duration duration = Duration.between(startTime, Instant.now());
            LOGGER.info("Export completed in {} seconds", duration.getSeconds());
            
            // Create completion marker to indicate successful export
            createCompletionMarker();
            
            LOGGER.info("================== Export Finished =====================");

        } catch (IOException | InterruptedException e) {
            LOGGER.error("Error during repository download", e);
        }
    }

    @Override
    public void run() {
        checkState(executorService != null, "Executor not initialized");
        LOGGER.info("Running initial download task using {} for comprehensive file discovery", apiMode);
        pendingAssetDiscoveryTasks.incrementAndGet();
        
        switch (apiMode) {
            case SEARCH_API:
                executorService.submit(new DownloadAssetsTask(null));
                break;
            case ASSETS_API:
                executorService.submit(new DownloadAssetsViaAssetsApiTask(null));
                break;
            case HYBRID:
                // Start with Assets API for comprehensive listing
                executorService.submit(new DownloadAssetsViaAssetsApiTask(null));
                // Also run Search API to catch any additional metadata
                pendingAssetDiscoveryTasks.incrementAndGet();
                executorService.submit(new DownloadAssetsTask(null));
                break;
        }
    }

    void notifyProgress() {
        LOGGER.info("Progress update: Downloaded {} assets out of {} found (Active tasks: {}, Pending discovery: {})", 
                   assetProcessed.get(), assetFound.get(), activeTasks.get(), pendingAssetDiscoveryTasks.get());
    }

    private void createCompletionMarker() {
        try {
            Path markerFile = downloadPath.resolve(".nexus-export-complete");
            String markerContent = String.format(
                "Export completed successfully at %s%n" +
                "Repository: %s%n" +
                "Assets processed: %d%n" +
                "Assets found: %d%n" +
                "Nexus URL: %s%n" +
                "Download path: %s%n" +
                "Export tool version: nexus3-export-1.0%n",
                Instant.now().toString(), 
                repositoryId, 
                assetProcessed.get(), 
                assetFound.get(),
                url,
                downloadPath.toString()
            );
            
            Files.write(markerFile, markerContent.getBytes());
            LOGGER.info("✅ Created completion marker with {} assets processed: {}", assetProcessed.get(), markerFile);
        } catch (IOException e) {
            LOGGER.warn("Failed to create completion marker (export was successful but resume detection may not work): {}", e.getMessage());
        }
    }

    private class DownloadAssetsTask implements Runnable {
        private final String continuationToken;
        private final int attemptNumber;
        private static final int MAX_RETRIES = 5;
        private static final long BASE_DELAY_MS = 2000;

        public DownloadAssetsTask(String continuationToken) {
            this(continuationToken, 1);
        }

        public DownloadAssetsTask(String continuationToken, int attemptNumber) {
            this.continuationToken = continuationToken;
            this.attemptNumber = attemptNumber;
            activeTasks.incrementAndGet();
            // Note: pendingAssetDiscoveryTasks is incremented when the task is submitted, not when created
        }

        @Override
        public void run() {
            try {
                // Use Search API for better performance with large repositories
                UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(url)
                        .path("/service/rest/v1/search/assets")
                        .queryParam("repository", repositoryId);

                if (continuationToken != null) {
                    uriBuilder.queryParam("continuationToken", continuationToken);
                }

                String uri = uriBuilder.build().toUriString();
                LOGGER.info("Attempt {} to fetch assets using Search API: {}", attemptNumber, uri);
                
                ResponseEntity<Assets> response = restTemplate.getForEntity(uri, Assets.class);

                if (response.getBody() != null && response.getBody().getItems() != null) {
                    LOGGER.info("Successfully fetched {} assets on attempt {}", 
                              response.getBody().getItems().size(), attemptNumber);
                    
                    for (Item item : response.getBody().getItems()) {
                        // In hybrid mode, check for duplicates
                        if (apiMode == ApiMode.HYBRID) {
                            if (discoveredAssetIds.add(item.getId())) {
                                executorService.submit(new DownloadItemTask(item));
                                assetFound.incrementAndGet();
                            } else {
                                LOGGER.debug("Skipping duplicate asset from Search API: {}", item.getPath());
                            }
                        } else {
                            executorService.submit(new DownloadItemTask(item));
                            assetFound.incrementAndGet();
                        }
                    }

                    notifyProgress();

                    if (response.getBody().getContinuationToken() != null) {
                        pendingAssetDiscoveryTasks.incrementAndGet();
                        executorService.submit(new DownloadAssetsTask(response.getBody().getContinuationToken()));
                    }
                }
            } catch (HttpServerErrorException e) {
                if (attemptNumber < MAX_RETRIES && (e.getRawStatusCode() == 500 || e.getRawStatusCode() == 502 || e.getRawStatusCode() == 503)) {
                    long delayMs = BASE_DELAY_MS * (long) Math.pow(2, attemptNumber - 1);
                    LOGGER.warn("Attempt {} failed to list/submit download tasks, retrying in {} ms", 
                              attemptNumber, delayMs);
                    LOGGER.warn("Error details: {}", e.getMessage());
                    
                    try {
                        Thread.sleep(delayMs);
                        pendingAssetDiscoveryTasks.incrementAndGet();
                        executorService.submit(new DownloadAssetsTask(continuationToken, attemptNumber + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Interrupted while waiting to retry", ie);
                    }
                } else {
                    LOGGER.error("Failed to list assets after {} attempts. Giving up on this batch.", attemptNumber, e);
                }
            } catch (Exception e) {
                if (attemptNumber < MAX_RETRIES) {
                    long delayMs = BASE_DELAY_MS * (long) Math.pow(2, attemptNumber - 1);
                    LOGGER.warn("Attempt {} failed with unexpected error, retrying in {} ms", 
                              attemptNumber, delayMs);
                    LOGGER.warn("Error details: {}", e.getMessage());
                    
                    try {
                        Thread.sleep(delayMs);
                        pendingAssetDiscoveryTasks.incrementAndGet();
                        executorService.submit(new DownloadAssetsTask(continuationToken, attemptNumber + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Interrupted while waiting to retry", ie);
                    }
                } else {
                    LOGGER.error("Failed to list assets after {} attempts with unexpected error. Giving up on this batch.", attemptNumber, e);
                }
            } finally {
                activeTasks.decrementAndGet();
                pendingAssetDiscoveryTasks.decrementAndGet();
            }
        }
    }

    private class DownloadAssetsViaAssetsApiTask implements Runnable {
        private final String continuationToken;
        private final int attemptNumber;
        private static final int MAX_RETRIES = 5;
        private static final long BASE_DELAY_MS = 2000;

        public DownloadAssetsViaAssetsApiTask(String continuationToken) {
            this(continuationToken, 1);
        }

        public DownloadAssetsViaAssetsApiTask(String continuationToken, int attemptNumber) {
            this.continuationToken = continuationToken;
            this.attemptNumber = attemptNumber;
            activeTasks.incrementAndGet();
            // Note: pendingAssetDiscoveryTasks is incremented when the task is submitted, not when created
        }

        @Override
        public void run() {
            try {
                // Use Assets API for comprehensive listing
                UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(url)
                        .path("/service/rest/v1/assets")
                        .queryParam("repository", repositoryId);

                if (continuationToken != null) {
                    uriBuilder.queryParam("continuationToken", continuationToken);
                }

                String uri = uriBuilder.build().toUriString();
                LOGGER.info("Attempt {} to fetch assets using Assets API: {}", attemptNumber, uri);
                
                ResponseEntity<Assets> response = restTemplate.getForEntity(uri, Assets.class);

                if (response.getBody() != null && response.getBody().getItems() != null) {
                    LOGGER.info("Successfully fetched {} assets on attempt {}", 
                              response.getBody().getItems().size(), attemptNumber);
                    
                    for (Item item : response.getBody().getItems()) {
                        // In hybrid mode, check for duplicates
                        if (apiMode == ApiMode.HYBRID) {
                            if (discoveredAssetIds.add(item.getId())) {
                                executorService.submit(new DownloadItemTask(item));
                                assetFound.incrementAndGet();
                            } else {
                                LOGGER.debug("Skipping duplicate asset from Assets API: {}", item.getPath());
                            }
                        } else {
                            executorService.submit(new DownloadItemTask(item));
                            assetFound.incrementAndGet();
                        }
                    }

                    notifyProgress();

                    if (response.getBody().getContinuationToken() != null) {
                        pendingAssetDiscoveryTasks.incrementAndGet();
                        executorService.submit(new DownloadAssetsViaAssetsApiTask(response.getBody().getContinuationToken()));
                    }
                }
            } catch (HttpServerErrorException e) {
                if (attemptNumber < MAX_RETRIES && (e.getRawStatusCode() == 500 || e.getRawStatusCode() == 502 || e.getRawStatusCode() == 503)) {
                    long delayMs = BASE_DELAY_MS * (long) Math.pow(2, attemptNumber - 1);
                    LOGGER.warn("Attempt {} failed to list/submit download tasks, retrying in {} ms", 
                              attemptNumber, delayMs);
                    LOGGER.warn("Error details: {}", e.getMessage());
                    
                    try {
                        Thread.sleep(delayMs);
                        pendingAssetDiscoveryTasks.incrementAndGet();
                        executorService.submit(new DownloadAssetsViaAssetsApiTask(continuationToken, attemptNumber + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Interrupted while waiting to retry", ie);
                    }
                } else {
                    LOGGER.error("Failed to list assets after {} attempts. Giving up on this batch.", attemptNumber, e);
                }
            } catch (Exception e) {
                if (attemptNumber < MAX_RETRIES) {
                    long delayMs = BASE_DELAY_MS * (long) Math.pow(2, attemptNumber - 1);
                    LOGGER.warn("Attempt {} failed with unexpected error, retrying in {} ms", 
                              attemptNumber, delayMs);
                    LOGGER.warn("Error details: {}", e.getMessage());
                    
                    try {
                        Thread.sleep(delayMs);
                        pendingAssetDiscoveryTasks.incrementAndGet();
                        executorService.submit(new DownloadAssetsViaAssetsApiTask(continuationToken, attemptNumber + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOGGER.error("Interrupted while waiting to retry", ie);
                    }
                } else {
                    LOGGER.error("Failed to list assets after {} attempts with unexpected error. Giving up on this batch.", attemptNumber, e);
                }
            } finally {
                activeTasks.decrementAndGet();
                pendingAssetDiscoveryTasks.decrementAndGet();
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
                LOGGER.info("Downloading asset from: {}", item.getDownloadUrl());
                Path relativeItemPath = Paths.get(item.getPath()).normalize();
                if (relativeItemPath.isAbsolute()) {
                    LOGGER.warn("Asset path was absolute. Forcing to relative: {}", relativeItemPath);
                    relativeItemPath = Paths.get(".").resolve(item.getPath().substring(1)).normalize();
                }
                Path assetPath = downloadPath.resolve(relativeItemPath);
                Files.createDirectories(assetPath.getParent());
                URI downloadUri = URI.create(item.getDownloadUrl());
                int tryCount = 1;
                while (tryCount <= 3) {
                    try (InputStream assetStream = downloadUri.toURL().openStream()) {
                        Files.copy(assetStream, assetPath, StandardCopyOption.REPLACE_EXISTING);
                        HashCode hash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                        if (Objects.equals(hash.toString(), item.getChecksum().getSha1())) {
                            LOGGER.info("Successfully downloaded and verified: {}", item.getPath());
                            break;
                        }
                        tryCount++;
                        LOGGER.warn("Checksum mismatch, retrying download for: {}", item.getPath());
                    } catch (FileAlreadyExistsException e) {
                        LOGGER.warn("File already exists, skipping: {}", assetPath);
                        break;
                    }
                }
                assetProcessed.incrementAndGet();
                notifyProgress();
            } catch (IOException e) {
                LOGGER.error("Failed to download asset: {}", item.getDownloadUrl(), e);
            } finally {
                activeTasks.decrementAndGet();
            }
        }
    }
}
