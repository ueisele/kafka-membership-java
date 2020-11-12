package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.membership.exception.LeaderElectionInitializationException;

import java.util.Map;

public interface SimpleLeaderElectorBuilder {
    SimpleLeaderElector build(Map<String, ?> configs) throws LeaderElectionInitializationException;
}
