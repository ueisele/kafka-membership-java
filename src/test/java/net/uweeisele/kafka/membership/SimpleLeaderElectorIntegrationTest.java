package net.uweeisele.kafka.membership;

import com.google.common.collect.ImmutableMap;
import net.uweeisele.kafka.test.cluster.EmbeddedSingleNodeKafkaCluster;
import net.uweeisele.kafka.test.support.Assertion;
import net.uweeisele.kafka.test.support.SimpleLeaderElectionEventCollector;
import org.apache.kafka.clients.CommonClientConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Math.round;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleLeaderElectorIntegrationTest {

    static int ELECTOR_HEARTBEAT_INTERVAL_MS = 500;

    static AtomicInteger GROUP_COUNTER = new AtomicInteger(0);

    @RegisterExtension
    static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster(ImmutableMap.<String,String>builder()
            .put("group.initial.rebalance.delay.ms", "0")
            .build());

    Map<SimpleLeaderElector, SimpleLeaderElectionEventCollector> leaderElectors = new ConcurrentHashMap<>();

    @Test
    void shouldBeElectedAsLeaderIfSingleMember() throws InterruptedException {
        String groupId = newGroupId();

        SimpleLeaderElector elector = createLeaderElector(groupId);
        elector.joinElection();

        Optional<ElectionGroupGeneration> generationA = elector.awaitElectionGroupJoined(10, SECONDS);
        assertThat(generationA.isEmpty(), is(false));
        assertThat(generationA.get().getGroupId(), is(groupId));
        assertThat(generationA.get().getGeneration(), is(1));

        Optional<ElectionGroupGeneration> generationB = elector.awaitLeadership(10, SECONDS);
        assertThat(generationB.isEmpty(), is(false));
        assertThat(generationB.get().getGroupId(), is(groupId));
        assertThat(generationB.get().getGeneration(), is(1));
        assertThat(generationB.get().isLeader(), is(true));

        elector.close();
    }

    @Test
    void shouldBeElectedAsFollowerIfSecondMember() throws InterruptedException, TimeoutException {
        String groupId = newGroupId();

        SimpleLeaderElector electorA = createLeaderElector(groupId);
        electorA.joinElection();
        collector(electorA).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(1));
        });

        SimpleLeaderElector electorB = createLeaderElector(groupId);
        electorB.joinElection();
        collector(electorB).poll(gen -> {
            assertTrue(gen.isFollower());
            assertThat(gen.getGeneration(), is(2));
        });
        collector(electorB).poll(Assertion::isEmpty, ofMillis(round(ELECTOR_HEARTBEAT_INTERVAL_MS * 1.5)));
        collector(electorA).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(2));
        });
        collector(electorA).poll(Assertion::isEmpty, ofMillis(round(ELECTOR_HEARTBEAT_INTERVAL_MS * 1.5)));

        electorB.close();
        electorA.close();
    }

    @Test
    void shouldBeElectedAsLeaderIfLeaderLeaveGroup() throws InterruptedException, TimeoutException {
        String groupId = newGroupId();

        SimpleLeaderElector electorA = createLeaderElector(groupId);
        electorA.joinElection();
        collector(electorA).poll(gen -> assertTrue(gen.isLeader()));

        SimpleLeaderElector electorB = createLeaderElector(groupId);
        electorB.joinElection();
        collector(electorB).poll(gen -> assertTrue(gen.isFollower()));
        collector(electorA).poll(gen -> assertTrue(gen.isLeader()));

        electorA.close();
        collector(electorA).poll(Assertion::isEmpty, ofMillis(round(ELECTOR_HEARTBEAT_INTERVAL_MS * 1.5)));
        collector(electorB).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(3));
        });

        electorB.close();
        collector(electorB).poll(Assertion::isEmpty, ofMillis(round(ELECTOR_HEARTBEAT_INTERVAL_MS * 1.5)));
    }

    @Test
    void shouldBeAbleToJoinAfterAnotherMemberLeavesGroup() throws InterruptedException, TimeoutException {
        String groupId = newGroupId();

        SimpleLeaderElector electorA = createLeaderElector(groupId);
        electorA.joinElection();
        collector(electorA).poll(gen -> assertThat(gen.getGeneration(), is(1)));

        SimpleLeaderElector electorB = createLeaderElector(groupId);
        electorB.joinElection();
        collector(electorB).poll(gen -> assertThat(gen.getGeneration(), is(2)));
        collector(electorA).poll(gen -> assertThat(gen.getGeneration(), is(2)));

        electorA.close();
        collector(electorB).poll(gen -> assertThat(gen.getGeneration(), is(3)));

        SimpleLeaderElector electorC = createLeaderElector(groupId);
        electorC.joinElection();
        collector(electorC).poll(gen -> {
            assertTrue(gen.isFollower());
            assertThat(gen.getGeneration(), is(4));
        });
        collector(electorB).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(4));
        });

        electorC.close();
        electorB.close();
    }

    @Test
    void shouldHaveALeaderForEachGroup() throws InterruptedException, TimeoutException {
        String groupId1 = newGroupId();
        SimpleLeaderElector electorAG1 = createLeaderElector(groupId1);
        electorAG1.joinElection();
        collector(electorAG1).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(1));
        });

        String groupId2 = newGroupId();
        SimpleLeaderElector electorAG2 = createLeaderElector(groupId2);
        electorAG2.joinElection();
        collector(electorAG2).poll(gen -> {
            assertTrue(gen.isLeader());
            assertThat(gen.getGeneration(), is(1));
        });

        electorAG1.close();
        electorAG2.close();
    }

    String newGroupId() {
        return "group" + GROUP_COUNTER.incrementAndGet();
    }

    SimpleLeaderElector createLeaderElector(String groupId) {
        SimpleLeaderElectionEventCollector collector = new SimpleLeaderElectionEventCollector();
        SimpleLeaderElector leaderElector = new SimpleLeaderElector(leaderElectorConfigs(groupId)).addLeaderElectionListener(collector);
        leaderElectors.put(leaderElector, collector);
        return leaderElector;
    }

    Map<String, Object> leaderElectorConfigs(String groupId) {
        return ImmutableMap.<String,Object>builder()
                .put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
                .put(SimpleLeaderElectorConfig.GROUP_ID_CONFIG, groupId)
                .put(SimpleLeaderElectorConfig.HEARTBEAT_INTERVAL_MS_CONFIG, ELECTOR_HEARTBEAT_INTERVAL_MS)
                .build();
    }

    SimpleLeaderElectionEventCollector collector(SimpleLeaderElector leaderElector) {
        return leaderElectors.get(leaderElector);
    }

    @AfterEach
    void cleanUp() {
        leaderElectors.forEach((elector, c) -> elector.close());
        leaderElectors.clear();
    }

}
