// Repository.java
package fr.kage.nexus3;

import java.util.Map;

public class Repository {
    private String name;
    private String format;
    private String type;
    private String url;
    private boolean online;
    private Map<String, Object> storage;
    private Map<String, Object> cleanup;
    private Map<String, Object> proxy;
    private Map<String, Object> negativeCache;
    private Map<String, Object> httpClient;
    private String routingRule;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public Map<String, Object> getStorage() {
        return storage;
    }

    public void setStorage(Map<String, Object> storage) {
        this.storage = storage;
    }

    public Map<String, Object> getCleanup() {
        return cleanup;
    }

    public void setCleanup(Map<String, Object> cleanup) {
        this.cleanup = cleanup;
    }

    public Map<String, Object> getProxy() {
        return proxy;
    }

    public void setProxy(Map<String, Object> proxy) {
        this.proxy = proxy;
    }

    public Map<String, Object> getNegativeCache() {
        return negativeCache;
    }

    public void setNegativeCache(Map<String, Object> negativeCache) {
        this.negativeCache = negativeCache;
    }

    public Map<String, Object> getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(Map<String, Object> httpClient) {
        this.httpClient = httpClient;
    }

    public String getRoutingRule() {
        return routingRule;
    }

    public void setRoutingRule(String routingRule) {
        this.routingRule = routingRule;
    }
}

// RepositoryList.java
package fr.kage.nexus3;

import java.util.List;

public class RepositoryList {
    private List<Repository> repositories;

    public List<Repository> getRepositories() {
        return repositories;
    }

    public void setRepositories(List<Repository> repositories) {
        this.repositories = repositories;
    }
}