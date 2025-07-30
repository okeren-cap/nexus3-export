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

            LOGGER.info("Submitting initial task to executor using Search API for better performance.");
            executorService.submit(new DownloadRepository.DownloadAssetsTask(null));

            while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                if (activeTasks.get() == 0) {
                    LOGGER.info("All download tasks completed. Shutting down executor.");
                    executorService.shutdown();
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
        LOGGER.info("Running initial download task using Search API for better performance");
        executorService.submit(new DownloadRepository.DownloadAssetsTask(null));
    }

    void notifyProgress() {
        LOGGER.info("Progress update: Downloaded {} assets out of {} found", assetProcessed.get(), assetFound.get());
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
                        executorService.submit(new DownloadItemTask(item));
                        assetFound.incrementAndGet();
                    }

                    notifyProgress();

                    if (response.getBody().getContinuationToken() != null) {
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
