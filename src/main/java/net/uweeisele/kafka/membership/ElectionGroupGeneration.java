package net.uweeisele.kafka.membership;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ElectionGroupGeneration {

    private final String groupId;
    private final String localMemberId;
    private final String leaderId;
    private final int generation;

    public ElectionGroupGeneration(String groupId, String localMemberId, String leaderId, int generation) {
        this.groupId = requireNonNull(groupId);
        this.localMemberId = requireNonNull(localMemberId);
        this.leaderId = requireNonNull(leaderId);
        this.generation = generation;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getLocalMemberId() {
        return localMemberId;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public int getGeneration() {
        return generation;
    }

    public boolean isJoined() {
        return generation >= 0;
    }

    public boolean isLeader() {
        return isJoined() && localMemberId.equals(leaderId);
    }

    public boolean isFollower() {
        return isJoined() && !isLeader();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElectionGroupGeneration that = (ElectionGroupGeneration) o;
        return generation == that.generation &&
                Objects.equals(groupId, that.groupId) &&
                Objects.equals(localMemberId, that.localMemberId) &&
                Objects.equals(leaderId, that.leaderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupId, localMemberId, leaderId, generation);
    }
}
