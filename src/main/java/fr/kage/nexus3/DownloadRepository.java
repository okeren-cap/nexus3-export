// Enhanced DownloadRepository.java with timeout handling and retry logic

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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DownloadRepository implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadRepository.class);
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_DELAY_SECONDS = 30;
    private static final int BATCH_SIZE = 100; // Limit batch size for large repos

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

            LOGGER.info("Starting download of Nexus 3 repository '{}' into '{}'", repositoryId, downloadPath);
            executorService = Executors.newFixedThreadPool(5); // Reduced thread pool size

            if (authenticate) {
                LOGGER.info("Authentication enabled. Configuring credentials for REST and stream download.");
                // Increased timeouts for large repositories
                restTemplate = new RestTemplateBuilder()
                    .basicAuthentication(username, password)
                    .setConnectTimeout(Duration.ofSeconds(30))
                    .setReadTimeout(Duration.ofSeconds(120)) // Increased read timeout
                    .build();
                Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray());
                    }
                });
            } else {
                LOGGER.info("Authentication not required.");
                restTemplate = new RestTemplateBuilder()
                    .setConnectTimeout(Duration.ofSeconds(30))
                    .setReadTimeout(Duration.ofSeconds(120))
                    .build();
            }

            LOGGER.info("Submitting initial task to executor.");
            executorService.submit(new DownloadAssetsTask(null));

            while (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                if (activeTasks.get() == 0) {
                    LOGGER.info("All download tasks completed. Shutting down executor.");
                    executorService.shutdown();
                }
            }

            Duration duration = Duration.between(startTime, Instant.now());
            LOGGER.info("Export completed in {} seconds", duration.toSeconds());
            LOGGER.info("================== Export Finished =====================");

        } catch (IOException | InterruptedException e) {
            LOGGER.error("Error during repository download", e);
        }
    }

    @Override
    public void run() {
        checkState(executorService != null, "Executor not initialized");
        LOGGER.info("Running initial download task");
        executorService.submit(new DownloadAssetsTask(null));
    }

    void notifyProgress() {
        LOGGER.info("Progress update: Downloaded {} assets out of {} found", assetProcessed.get(), assetFound.get());
    }

    private class DownloadAssetsTask implements Runnable {
        private final String continuationToken;

        public DownloadAssetsTask(String continuationToken) {
            this.continuationToken = continuationToken;
            activeTasks.incrementAndGet();
        }

        @Override
        public void run() {
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
                    LOGGER.info("Fetching assets batch {} (retry {})", continuationToken != null ? "with continuation" : "initial", retryCount + 1);
                    
                    ResponseEntity<Assets> response = restTemplate.getForEntity(uri, Assets.class);

                    if (response.getBody() != null && response.getBody().getItems() != null) {
                        int itemCount = response.getBody().getItems().size();
                        LOGGER.info("Retrieved {} assets in this batch", itemCount);
                        
                        for (Item item : response.getBody().getItems()) {
                            executorService.submit(new DownloadItemTask(item));
                            assetFound.incrementAndGet();
                        }

                        notifyProgress();

                        if (response.getBody().getContinuationToken() != null) {
                            // Add a small delay before next batch to reduce server load
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                            executorService.submit(new DownloadAssetsTask(response.getBody().getContinuationToken()));
                        }
                    }
                    
                    // Success - break out of retry loop
                    break;
                    
                } catch (HttpServerErrorException.InternalServerError e) {
                    retryCount++;
                    if (e.getMessage().contains("Timed out reading query result")) {
                        LOGGER.warn("Server timeout occurred (attempt {}/{}). Retrying in {} seconds...", 
                                   retryCount, MAX_RETRIES, RETRY_DELAY_SECONDS);
                        
                        if (retryCount < MAX_RETRIES) {
                            try {
                                Thread.sleep(RETRY_DELAY_SECONDS * 1000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        } else {
                            LOGGER.error("Max retries exceeded for assets batch. Skipping this batch.", e);
                        }
                    } else {
                        LOGGER.error("Non-timeout server error occurred", e);
                        break;
                    }
                } catch (Exception e) {
                    LOGGER.error("Failed to list or submit download tasks", e);
                    break;
                }
            }
            
            activeTasks.decrementAndGet();
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
                LOGGER.debug("Downloading asset from: {}", item.getDownloadUrl());
                Path relativeItemPath = Paths.get(item.getPath()).normalize();
                if (relativeItemPath.isAbsolute()) {
                    LOGGER.warn("Asset path was absolute. Forcing to relative: {}", relativeItemPath);
                    relativeItemPath = Paths.get(".").resolve(item.getPath().substring(1)).normalize();
                }
                Path assetPath = downloadPath.resolve(relativeItemPath);
                
                // Check if file already exists and has correct checksum
                if (Files.exists(assetPath)) {
                    try {
                        HashCode existingHash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                        if (Objects.equals(existingHash.toString(), item.getChecksum().getSha1())) {
                            LOGGER.debug("File already exists with correct checksum, skipping: {}", item.getPath());
                            assetProcessed.incrementAndGet();
                            return;
                        }
                    } catch (IOException e) {
                        LOGGER.debug("Error checking existing file, will re-download: {}", item.getPath());
                    }
                }
                
                Files.createDirectories(assetPath.getParent());
                URI downloadUri = URI.create(item.getDownloadUrl());
                int tryCount = 1;
                while (tryCount <= 3) {
                    try (InputStream assetStream = downloadUri.toURL().openStream()) {
                        Files.copy(assetStream, assetPath, StandardCopyOption.REPLACE_EXISTING);
                        HashCode hash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                        if (Objects.equals(hash.toString(), item.getChecksum().getSha1())) {
                            LOGGER.debug("Successfully downloaded and verified: {}", item.getPath());
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
                
                if (assetProcessed.get() % 100 == 0) {
                    notifyProgress();
                }
            } catch (IOException e) {
                LOGGER.error("Failed to download asset: {}", item.getDownloadUrl(), e);
            } finally {
                activeTasks.decrementAndGet();
            }
        }
    }
}