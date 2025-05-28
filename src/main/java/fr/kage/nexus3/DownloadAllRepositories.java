package fr.kage.nexus3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

public class DownloadAllRepositories {

	private static final Logger LOGGER = LoggerFactory.getLogger(DownloadAllRepositories.class);

	private final String url;
	private final String baseDownloadPath;
	private final boolean authenticate;
	private final String username;
	private final String password;

	public DownloadAllRepositories(String url, String baseDownloadPath, boolean authenticate, String username, String password) {
		this.url = url;
		this.baseDownloadPath = baseDownloadPath;
		this.authenticate = authenticate;
		this.username = username;
		this.password = password;
	}

	public void start() {
		LOGGER.info("Fetching all repositories from Nexus instance: {}", url);

		RestTemplate restTemplate = authenticate
				? new RestTemplateBuilder().basicAuthentication(username, password).build()
				: new RestTemplate();

		String repoListUrl = url + "/service/rest/v1/repositories";

		ResponseEntity<List> response = restTemplate.exchange(
				repoListUrl, HttpMethod.GET, null, List.class
		);

		List<Map<String, Object>> repositories = response.getBody();

		if (repositories == null || repositories.isEmpty()) {
			LOGGER.warn("⚠ Keine Repositories gefunden.");
			return;
		}

		for (Map<String, Object> repo : repositories) {
			String repoName = (String) repo.get("name");
			LOGGER.info("▶ Exporting repository: {}", repoName);
			try {
				new DownloadRepository(
						url,
						repoName,
						baseDownloadPath,
						authenticate,
						username,
						password
				).start();
			} catch (Exception e) {
				LOGGER.error("❌ Fehler beim Export von '{}'", repoName, e);
			}
		}
	}
}
