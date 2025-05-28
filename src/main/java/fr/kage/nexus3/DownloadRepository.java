// src/main/java/fr/kage/nexus3/DownloadRepository.java

package fr.kage.nexus3;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        this.downloadPath = downloadPath == null ? null : Paths.get(downloadPath);
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
                downloadPath = Files.createTempDirectory("nexus3");
                LOGGER.info("No path specified. Using temporary directory: {}", downloadPath);
            } else {
                if (!Files.exists(downloadPath)) {
                    LOGGER.info("Creating specified download directory: {}", downloadPath);
                    Files.createDirectories(downloadPath);
                }
                if (!Files.isDirectory(downloadPath) || !Files.isWritable(downloadPath)) {
                    throw new IOException("Not a writable directory: " + downloadPath);
                }
            }

            LOGGER.info("Starting download of Nexus 3 repository '{}' into '{}'", repositoryId, downloadPath);
            executorService = Executors.newFixedThreadPool(10);

            if (authenticate) {
                LOGGER.info("Authentication enabled. Configuring credentials for REST and stream download.");
                restTemplate = new RestTemplateBuilder().basicAuthentication(username, password).build();
                Authenticator.setDefault(new Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, password.toCharArray());
                    }
                });
            } else {
                LOGGER.info("Authentication not required.");
                restTemplate = new RestTemplate();
            }

            LOGGER.info("Submitting initial task to executor.");
            executorService.submit(this);

            while (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
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

        private String continuationToken;

        public DownloadAssetsTask(String continuationToken) {
            this.continuationToken = continuationToken;
            activeTasks.incrementAndGet();
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Requesting assets list{}", continuationToken != null ? " (with continuationToken)" : "");
                UriComponentsBuilder getAssets = UriComponentsBuilder.fromHttpUrl(url)
                        .pathSegment("service", "rest", "v1", "assets")
                        .queryParam("repository", repositoryId);
                if (continuationToken != null)
                    getAssets.queryParam("continuationToken", continuationToken);

                ResponseEntity<Assets> assetsEntity = restTemplate.getForEntity(getAssets.build().toUri(), Assets.class);
                Assets assets = assetsEntity.getBody();

                LOGGER.info("{} assets retrieved.", assets.getItems().size());

                if (assets.getContinuationToken() != null) {
                    LOGGER.info("Continuation token found, queuing next batch.");
                    executorService.submit(new DownloadAssetsTask(assets.getContinuationToken()));
                }

                assetFound.addAndGet(assets.getItems().size());
                notifyProgress();
                for (Item item : assets.getItems()) {
                    LOGGER.info("Queuing download task for asset: {}", item.getPath());
                    executorService.submit(new DownloadItemTask(item));
                }

            } catch (Exception e) {
                LOGGER.error("Asset download failed", e);
                executorService.shutdownNow();
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
                Path assetPath = downloadPath.resolve(item.getPath());
                Files.createDirectories(assetPath.getParent());
                URI downloadUri = URI.create(item.getDownloadUrl());
                int tryCount = 1;
                while (tryCount <= 3) {
                    try (InputStream assetStream = downloadUri.toURL().openStream()) {
                        Files.copy(assetStream, assetPath);
                        HashCode hash = com.google.common.io.Files.asByteSource(assetPath.toFile()).hash(Hashing.sha1());
                        if (Objects.equals(hash.toString(), item.getChecksum().getSha1())) {
                            LOGGER.info("Successfully downloaded and verified: {}", item.getPath());
                            break;
                        }
                        tryCount++;
                        LOGGER.warn("Checksum mismatch, retrying download for: {}", item.getPath());
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
