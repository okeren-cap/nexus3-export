package fr.kage.nexus3;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class RepositoryLister {
    private final RestTemplate restTemplate;
    private final String url;

    public RepositoryLister(String url, RestTemplate restTemplate) {
        this.url = url;
        this.restTemplate = restTemplate;
    }

    public List<String> listRepositories() {
        URI uri = UriComponentsBuilder
            .fromHttpUrl(url)
            .path("/service/rest/v1/repositories")
            .build()
            .toUri();

        ResponseEntity<RepositoryInfo[]> response = restTemplate.getForEntity(uri, RepositoryInfo[].class);
        return Arrays.stream(response.getBody())
                     .map(RepositoryInfo::getName)
                     .collect(Collectors.toList());
    }

    static class RepositoryInfo {
        private String name;
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }
}
