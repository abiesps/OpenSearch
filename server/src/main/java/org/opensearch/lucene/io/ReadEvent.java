package org.opensearch.lucene.io;

import java.util.Objects;

public class ReadEvent {
    private final String resourceDescription;
    private String queryId;
    private int shardId;
    private String threadName;
    private long readTime;
    private String fileName;
    private String segGen;
    private String phaseName;


    public ReadEvent(int shardId, String threadName, long readTime, String fileName,
                     String segGen, String phaseName, String queryId, String resourceDescription) {
        this.shardId = shardId;
        this.threadName = threadName;
        this.readTime = readTime;
        this.fileName = fileName;
        this.segGen = segGen;
        this.phaseName = phaseName;
        this.queryId = queryId;
        this.resourceDescription = resourceDescription;
    }

    // Getters
    public String getQueryId() { return queryId; }
    public int getShardId() { return shardId; }
    public String getThreadName() { return threadName; }
    public long getReadTime() { return readTime; }
    public String getFileName() { return fileName; }
    public String getSegGen() { return segGen; }
    public String getPhaseName() { return phaseName; }

    // Setters
    public void setQueryId(String queryId) { this.queryId = queryId; }
    public void setShardId(int shardId) { this.shardId = shardId; }
    public void setThreadName(String threadName) { this.threadName = threadName; }
    public void setReadTime(long readTime) { this.readTime = readTime; }
    public void setFileName(String fileName) { this.fileName = fileName; }
    public void setSegGen(String segGen) { this.segGen = segGen; }
    public void setPhaseName(String phaseName) { this.phaseName = phaseName; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReadEvent that = (ReadEvent) o;
        return shardId == that.shardId
            && Objects.equals(threadName, that.threadName)
            && Objects.equals(fileName, that.fileName)
            && Objects.equals(segGen, that.segGen)
            && Objects.equals(phaseName, that.phaseName)
            && Objects.equals(queryId, that.queryId)
        && Objects.equals(resourceDescription, that.resourceDescription);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, threadName, fileName, segGen, phaseName, queryId, resourceDescription);
    }

    @Override
    public String toString() {
        return "ReadEvent{" +
            "queryId='" + queryId + '\'' +
            ", shardId=" + shardId +
            ", threadName='" + threadName + '\'' +
            ", readTime=" + readTime +
            ", fileName='" + fileName + '\'' +
            ", segGen='" + segGen + '\'' +
            ", phaseName='" + phaseName + '\'' +
            ", resourceDesc='" + resourceDescription + '\'' +
            '}';
    }
}
