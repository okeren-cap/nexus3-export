package fr.kage.nexus3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DownloadAllRepositories {

	private static final Logger LOGGER = LoggerFactory.getLogger(DownloadAllRepositories.class);

	private final String url;
	private final String baseDownloadPath;
	private final boolean authenticate;
	private final String username;
	private final String password;
	private final boolean forceRedownload;

	public DownloadAllRepositories(String url, String baseDownloadPath, boolean authenticate, String username, String password) {
		this(url, baseDownloadPath, authenticate, username, password, false);
	}

	public DownloadAllRepositories(String url, String baseDownloadPath, boolean authenticate, String username, String password, boolean forceRedownload) {
		this.url = url;
		this.baseDownloadPath = baseDownloadPath;
		this.authenticate = authenticate;
		this.username = username;
		this.password = password;
		this.forceRedownload = forceRedownload;
	}

	public void start() {
		LOGGER.info("================= Nexus 3 Bulk Export Start =================");
		LOGGER.info("Nexus URL: {}", url);
		LOGGER.info("Base download path: {}", baseDownloadPath);
		LOGGER.info("Force re-download: {}", forceRedownload);
		LOGGER.info("==============================================================");

		RestTemplate restTemplate = authenticate
				? new RestTemplateBuilder()
						.basicAuthentication(username, password)
						.setConnectTimeout(Duration.ofMinutes(2))
						.setReadTimeout(Duration.ofMinutes(5))
						.build()
				: new RestTemplateBuilder()
						.setConnectTimeout(Duration.ofMinutes(2))
						.setReadTimeout(Duration.ofMinutes(5))
						.build();

		String repoListUrl = url + "/service/rest/v1/repositories";

		LOGGER.info("Fetching repository list from: {}", repoListUrl);
		ResponseEntity<List> response = restTemplate.exchange(
				repoListUrl, HttpMethod.GET, null, List.class
		);

		List<Map<String, Object>> repositories = response.getBody();

		if (repositories == null || repositories.isEmpty()) {
			LOGGER.warn("⚠ No repositories found.");
			return;
		}

		LOGGER.info("Found {} repositories to process", repositories.size());
		
		int processed = 0;
		int skipped = 0;
		int failed = 0;

		for (Map<String, Object> repo : repositories) {
			String repoName = (String) repo.get("name");
			
			try {
				if (!forceRedownload && isRepositoryAlreadyDownloaded(repoName)) {
					LOGGER.info("⏭ Skipping '{}' - already completed successfully (use --force to re-download)", repoName);
					skipped++;
					continue;
				}
				
				// Check for partial downloads from old system (no completion marker but has files)
				if (!forceRedownload && hasPartialDownload(repoName)) {
					LOGGER.warn("⚠ Found partial download for '{}' - will complete the download", repoName);
				}

				LOGGER.info("▶ [{}/{} remaining] Exporting repository: {}", 
					repositories.size() - processed - skipped, repositories.size(), repoName);
				
				new DownloadRepository(
						url,
						repoName,
						baseDownloadPath,
						authenticate,
						username,
						password
				).start();
				
				processed++;
				LOGGER.info("✅ Successfully exported '{}' ({} processed, {} skipped, {} failed)", 
					repoName, processed, skipped, failed);
				
			} catch (Exception e) {
				failed++;
				LOGGER.error("❌ Failed to export '{}' ({} processed, {} skipped, {} failed)", 
					repoName, processed, skipped, failed, e);
			}
		}

		LOGGER.info("================= Bulk Export Summary ===================");
		LOGGER.info("Total repositories: {}", repositories.size());
		LOGGER.info("Successfully processed: {}", processed);
		LOGGER.info("Skipped (already downloaded): {}", skipped);
		LOGGER.info("Failed: {}", failed);
		LOGGER.info("=========================================================");
	}

	private boolean isRepositoryAlreadyDownloaded(String repoName) {
		try {
			Path repoPath = Paths.get(baseDownloadPath).resolve(repoName);
			Path completionMarker = repoPath.resolve(".nexus-export-complete");
			
			if (Files.exists(completionMarker)) {
				try {
					// Read completion marker to show completion info
					String markerContent = new String(Files.readAllBytes(completionMarker));
					String[] lines = markerContent.split("\n");
					String completionTime = lines.length > 0 ? lines[0] : "unknown time";
					String assetCount = "unknown";
					
					// Extract asset count if available
					for (String line : lines) {
						if (line.startsWith("Assets processed:")) {
							assetCount = line.substring("Assets processed: ".length()).trim();
							break;
						}
					}
					
					LOGGER.debug("Repository '{}' completed at {} with {} assets", repoName, completionTime, assetCount);
					return true;
				} catch (IOException e) {
					LOGGER.warn("Completion marker exists for '{}' but cannot read it, assuming downloaded", repoName);
					return true;
				}
			}
			
			return false;
		} catch (Exception e) {
			LOGGER.warn("Error checking completion marker for repository '{}': {}", repoName, e.getMessage());
			return false;
		}
	}

	private boolean hasPartialDownload(String repoName) {
		try {
			Path repoPath = Paths.get(baseDownloadPath).resolve(repoName);
			Path completionMarker = repoPath.resolve(".nexus-export-complete");
			
			// Has files but no completion marker = partial download
			if (Files.exists(repoPath) && Files.isDirectory(repoPath) && !Files.exists(completionMarker)) {
				try {
					return Files.list(repoPath).findAny().isPresent();
				} catch (IOException e) {
					return false;
				}
			}
			
			return false;
		} catch (Exception e) {
			return false;
		}
	}
}
