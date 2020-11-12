package net.uweeisele.kafka.membership;

import com.google.common.collect.ImmutableMap;
import net.uweeisele.kafka.test.cluster.EmbeddedSingleNodeKafkaCluster;
import net.uweeisele.kafka.test.support.RunnableWithThrows;
import net.uweeisele.kafka.test.support.SimpleLeaderElectionEventCollector;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.impl.ExecutionContextImpl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

class DistributedLockIntegrationTest {

    static Logger LOG = LoggerFactory.getLogger(DistributedLockIntegrationTest.class);

    static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);
    static int ELECTOR_HEARTBEAT_INTERVAL_MS = 500;

    static AtomicInteger GROUP_COUNTER = new AtomicInteger(0);

    @RegisterExtension
    static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(ImmutableMap.<String,String>builder()
            .put("group.initial.rebalance.delay.ms", "0")
            .build());

    static final DistributedLockBuilder DISTRIBUTED_LOCK_BUILDER = new DistributedLockBuilder(distributedLockConfigs(), leaderElectorBuilder());
    static ConcurrentMap<Thread, Pair<SimpleLeaderElector, SimpleLeaderElectionEventCollector>> ELECTORS = new ConcurrentHashMap<>();
    static ConcurrentMap<String, Pair<Thread, ExecutorService>> EXECUTORS = new ConcurrentHashMap<>();

    @Test
    void shouldGetLockIfSingleMember() {
        String groupId = newGroupId();

        DistributedLock lock = distributedLock(groupId);

    }

    static String newGroupId() {
        return "group" + GROUP_COUNTER.incrementAndGet();
    }

    static DistributedLock distributedLock(String groupId) {
        return DISTRIBUTED_LOCK_BUILDER.distributedLock(groupId);
    }

    static SimpleLeaderElectorBuilder leaderElectorBuilder() {
        return configs -> {
            SimpleLeaderElectionEventCollector collector = new SimpleLeaderElectionEventCollector(DEFAULT_TIMEOUT);
            SimpleLeaderElector elector = new SimpleLeaderElector(configs).addLeaderElectionListener(collector);
            ELECTORS.put(Thread.currentThread(), ImmutablePair.of(elector, collector));
            return elector;
        };
    }

    static Map<String, Object> distributedLockConfigs() {
        return ImmutableMap.<String,Object>builder()
                .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
                .put(SimpleLeaderElectorConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ELECTOR_HEARTBEAT_INTERVAL_MS)
                .build();
    }

    static SimpleLeaderElector elector(Thread thread) throws InterruptedException {
        return getBlocking(ELECTORS, thread, DEFAULT_TIMEOUT).getLeft();
    }

    static SimpleLeaderElectionEventCollector collector(Thread thread) throws InterruptedException {
        return getBlocking(ELECTORS, thread, DEFAULT_TIMEOUT).getRight();

    }

    static <K,V> V getBlocking(Map<K,V> map, K key, Duration timeout) throws InterruptedException {
        V value;
        Duration effectiveTimeout = timeout;
        value = map.get(key);
        while (value == null && effectiveTimeout.toMillis() > 0) {
            long start = System.currentTimeMillis();
            MILLISECONDS.sleep(500);
            effectiveTimeout = effectiveTimeout.minusMillis(System.currentTimeMillis() - start);
            value = map.get(key);
        }
        return value;
    }

    static Thread run(String executorName, RunnableWithThrows runnable) {
        Pair<Thread, ExecutorService> entry = EXECUTORS.get(executorName);
        if (entry == null) {
            ThreadFactory threadFactory = new QueryableThreadFactory(Executors.defaultThreadFactory());
            ExecutorService executorService = Executors.newFixedThreadPool(1, threadFactory);
        }
        entry.getRight().submit(() -> {
            runnable.run();
            return null;
        });
        return entry.getLeft();
    }

    @AfterEach
    void cleanUp() {
        ELECTORS.forEach((thread, entry) -> entry.getLeft().close());
        ELECTORS.clear();
        EXECUTORS.forEach((g, entry) -> {
            entry.getLeft().interrupt();
            entry.getRight().shutdownNow();
            try {
                if (!entry.getRight().awaitTermination(5, SECONDS)) {
                    LOG.warn("Could not terminate al running threads.");
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted during test cleanUp.", e);
            }
        });
        EXECUTORS.clear();
    }

    static class QueryableThreadFactory implements ThreadFactory {

        private final ThreadFactory threadFactory;

        public QueryableThreadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
        }

        @Override
        public Thread newThread(Runnable r) {
            return null;
        }
    }

}