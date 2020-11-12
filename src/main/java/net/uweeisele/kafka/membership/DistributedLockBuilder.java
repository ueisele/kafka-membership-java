package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.membership.exception.LeaderElectionInitializationException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.CommonClientConfigs;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class DistributedLockBuilder {

    private final Map<String, ?> defaultConfigs;
    private final SimpleLeaderElectorBuilder leaderElectorBuilder;

    public DistributedLockBuilder(String bootstrapServer) {
        this(mapOfEntries(ImmutablePair.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)));
    }

    public DistributedLockBuilder(Map<String, ?> configs) {
        this(configs, SimpleLeaderElector::new);
    }

    public DistributedLockBuilder(Map<String, ?> configs, SimpleLeaderElectorBuilder leaderElectorBuilder) {
        this.defaultConfigs = new HashMap<>(configs);
        this.leaderElectorBuilder = leaderElectorBuilder;
    }

    public DistributedLock distributedLock(String groupId) throws LeaderElectionInitializationException {
        return distributedLock(groupId, new HashMap<>());
    }

    public DistributedLock distributedLock(String groupId, Map<String, ?> configs) throws LeaderElectionInitializationException {
        Map<String, Object> actualConfigs = new HashMap<>(configs);
        actualConfigs.put(SimpleLeaderElectorConfig.GROUP_ID_CONFIG, groupId);
        return distributedLock(actualConfigs);
    }

    public DistributedLock distributedLock(Map<String, ?> configs) throws LeaderElectionInitializationException {
        Map<String, Object> actualConfigs = new HashMap<>(defaultConfigs);
        actualConfigs.putAll(configs);
        return new DistributedLock(actualConfigs, leaderElectorBuilder);
    }

    @SafeVarargs
    private static Map<String,String> mapOfEntries(Map.Entry<String,String>... entries) {
        return Arrays.stream(entries).collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
