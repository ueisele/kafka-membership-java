package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.test.cluster.EmbeddedSingleNodeKafkaCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;

class DistributedLockIntegrationTest {

    @RegisterExtension
    static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    @Test
    void testLock() throws InterruptedException {

    }

    Lock distributedLock(String groupId) {
        return new DistributedLockBuilder(CLUSTER.bootstrapServers()).distributedLock(groupId);
    }

}
