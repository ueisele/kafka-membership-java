package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.membership.exception.LeaderElectionInitializationException;

import java.util.Map;

public interface LeaderElectorBuilder {
    SimpleLeaderElector buildLeaderElector(Map<String, ?> configs) throws LeaderElectionInitializationException;
}
