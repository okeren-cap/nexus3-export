// Enhanced Item.java (add lastModified field)
package fr.kage.nexus3;

import java.time.LocalDateTime;
import java.util.Map;

public class Item {
    private String downloadUrl;
    private String path;
    private String id;
    private String repository;
    private String format;
    private Checksum checksum;
    private LocalDateTime lastModified;
    private String lastDownloaded;
    private String uploader;
    private String uploaderIp;
    private long fileSize;
    private long blobCreated;
    private long lastUpdated;
    private Map<String, Object> attributes;

    // Existing getters and setters...
    public String getDownloadUrl() {
        return downloadUrl;
    }

    public void setDownloadUrl(String downloadUrl) {
        this.downloadUrl = downloadUrl;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRepository() {
        return repository;
    }

    public void setRepository(String repository) {
        this.repository = repository;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Checksum getChecksum() {
        return checksum;
    }

    public void setChecksum(Checksum checksum) {
        this.checksum = checksum;
    }

    // New getters and setters
    public LocalDateTime getLastModified() {
        return lastModified;
    }

    public void setLastModified(LocalDateTime lastModified) {
        this.lastModified = lastModified;
    }

    public String getLastDownloaded() {
        return lastDownloaded;
    }

    public void setLastDownloaded(String lastDownloaded) {
        this.lastDownloaded = lastDownloaded;
    }

    public String getUploader() {
        return uploader;
    }

    public void setUploader(String uploader) {
        this.uploader = uploader;
    }

    public String getUploaderIp() {
        return uploaderIp;
    }

    public void setUploaderIp(String uploaderIp) {
        this.uploaderIp = uploaderIp;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public long getBlobCreated() {
        return blobCreated;
    }

    public void setBlobCreated(long blobCreated) {
        this.blobCreated = blobCreated;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    public String getArtifactKey() {
        // For Maven repositories, use groupId:artifactId
        if ("maven2".equals(format) && attributes != null) {
            Map<String, Object> maven = (Map<String, Object>) attributes.get("maven2");
            if (maven != null) {
                String groupId = (String) maven.get("groupId");
                String artifactId = (String) maven.get("artifactId");
                if (groupId != null && artifactId != null) {
                    return groupId + ":" + artifactId;
                }
            }
        }
        
        // For npm repositories
        if ("npm".equals(format) && attributes != null) {
            Map<String, Object> npm = (Map<String, Object>) attributes.get("npm");
            if (npm != null) {
                String name = (String) npm.get("name");
                if (name != null) {
                    return name;
                }
            }
        }
        
        // For NuGet repositories
        if ("nuget".equals(format) && attributes != null) {
            Map<String, Object> nuget = (Map<String, Object>) attributes.get("nuget");
            if (nuget != null) {
                String id = (String) nuget.get("id");
                if (id != null) {
                    return id;
                }
            }
        }
        
        // Fallback to path-based grouping
        String pathWithoutVersion = path;
        if (pathWithoutVersion != null && pathWithoutVersion.contains("/")) {
            String[] parts = pathWithoutVersion.split("/");
            if (parts.length > 1) {
                // Remove the last part which is likely the versioned filename
                return String.join("/", java.util.Arrays.copyOf(parts, parts.length - 1));
            }
        }
        return pathWithoutVersion != null ? pathWithoutVersion : id;
    }

    public String getVersion() {
        // For Maven repositories
        if ("maven2".equals(format) && attributes != null) {
            Map<String, Object> maven = (Map<String, Object>) attributes.get("maven2");
            if (maven != null) {
                String version = (String) maven.get("version");
                if (version != null) {
                    return version;
                }
            }
        }
        
        // For npm repositories
        if ("npm".equals(format) && attributes != null) {
            Map<String, Object> npm = (Map<String, Object>) attributes.get("npm");
            if (npm != null) {
                String version = (String) npm.get("version");
                if (version != null) {
                    return version;
                }
            }
        }
        
        // For NuGet repositories
        if ("nuget".equals(format) && attributes != null) {
            Map<String, Object> nuget = (Map<String, Object>) attributes.get("nuget");
            if (nuget != null) {
                String version = (String) nuget.get("version");
                if (version != null) {
                    return version;
                }
            }
        }
        
        // Fallback: try to extract version from path
        if (path != null && path.contains("/")) {
            String[] parts = path.split("/");
            if (parts.length > 0) {
                String filename = parts[parts.length - 1];
                // Try to extract version pattern (numbers and dots)
                if (filename.matches(".*\\d+\\.\\d+.*")) {
                    return filename;
                }
            }
        }
        
        return String.valueOf(lastUpdated); // Use timestamp as fallback
    }
}
