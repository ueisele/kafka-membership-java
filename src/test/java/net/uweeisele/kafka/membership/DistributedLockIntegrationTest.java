package net.uweeisele.kafka.membership;

import com.google.common.collect.ImmutableMap;
import net.uweeisele.kafka.test.cluster.EmbeddedSingleNodeKafkaCluster;
import net.uweeisele.kafka.test.support.SimpleLeaderElectionEventCollector;
import net.uweeisele.kafka.test.support.execution.CompletableFutureExecutorService;
import net.uweeisele.kafka.test.support.execution.RunnableWithThrows;
import net.uweeisele.kafka.test.support.execution.WorkerExecutor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistributedLockIntegrationTest {

    static Logger LOG = LoggerFactory.getLogger(DistributedLockIntegrationTest.class);

    static Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);
    static int ELECTOR_HEARTBEAT_INTERVAL_MS = 500;

    static AtomicInteger GROUP_COUNTER = new AtomicInteger(0);

    @RegisterExtension
    static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(ImmutableMap.<String,String>builder()
            .put("group.initial.rebalance.delay.ms", "0")
            .build());

    final DistributedLockBuilder distributedLockBuilder = new DistributedLockBuilder(distributedLockConfigs(), leaderElectorBuilder());
    ConcurrentMap<Thread, Pair<SimpleLeaderElector, SimpleLeaderElectionEventCollector>> electors = new ConcurrentHashMap<>();
    ConcurrentMap<String, Pair<Thread, CompletableFutureExecutorService<?>>> workers = new ConcurrentHashMap<>();

    @Test
    void shouldGetLockIfSingleMember() throws InterruptedException, TimeoutException {
        String groupId = newGroupId();
        DistributedLock lock = distributedLock(groupId);

        assertTrue(lock.tryLock(DEFAULT_TIMEOUT.toMillis(), MILLISECONDS));
        collector(currentThread()).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(1));
        });

        lock.unlock();
        assertFalse(lock.isLocked());
        assertTrue(elector(currentThread()).isClosed());
    }

    static String newGroupId() {
        return "group" + GROUP_COUNTER.incrementAndGet();
    }

    DistributedLock distributedLock(String groupId) {
        return distributedLockBuilder.distributedLock(groupId);
    }

    SimpleLeaderElectorBuilder leaderElectorBuilder() {
        return configs -> {
            SimpleLeaderElectionEventCollector collector = new SimpleLeaderElectionEventCollector(DEFAULT_TIMEOUT);
            SimpleLeaderElector elector = new SimpleLeaderElector(configs).addLeaderElectionListener(collector);
            electors.put(currentThread(), ImmutablePair.of(elector, collector));
            return elector;
        };
    }

    static Map<String, Object> distributedLockConfigs() {
        return ImmutableMap.<String,Object>builder()
                .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
                .put(SimpleLeaderElectorConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ELECTOR_HEARTBEAT_INTERVAL_MS)
                .build();
    }

    SimpleLeaderElector elector(Thread thread) throws InterruptedException {
        return getBlocking(electors, thread, DEFAULT_TIMEOUT).getLeft();
    }

    SimpleLeaderElectionEventCollector collector(Thread thread) throws InterruptedException {
        return getBlocking(electors, thread, DEFAULT_TIMEOUT).getRight();

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

    CompletableFuture<?> runInThread(String threadName, RunnableWithThrows runnable) {
        return runInThread(threadName, () -> {
            runnable.run();
            return null;
        });
    }

    <V> CompletableFuture<V> runInThread(String threadName, Callable<V> runnable) {
        Pair<Thread, CompletableFutureExecutorService<?>> entry = workers.get(threadName);
        if (entry == null) {
            WorkerExecutor worker = new WorkerExecutor();
            Thread workerThread = new Thread(worker);
            workerThread.start();
            CompletableFutureExecutorService<?> executorService = new CompletableFutureExecutorService<>(worker);
            entry = ImmutablePair.of(workerThread, executorService);
            workers.put(threadName, entry);
        }
        return entry.getRight().submit(runnable);
    }

    @AfterEach
    void cleanUp() {
        electors.forEach((thread, entry) -> entry.getLeft().close());
        electors.clear();
        workers.forEach((g, entry) -> {
            entry.getRight().shutdownNow();
            entry.getLeft().interrupt();
            try {
                if (!entry.getRight().awaitTermination(Duration.ofSeconds(5))) {
                    LOG.warn("Could not terminate all running threads.");
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted during test cleanUp.", e);
            }
        });
        workers.clear();
    }

}