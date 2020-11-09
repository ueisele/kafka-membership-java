package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.test.cluster.EmbeddedSingleNodeKafkaCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class DistributedLockTest {

    static final Logger LOG = LoggerFactory.getLogger(DistributedLockTest.class);

    @RegisterExtension
    static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    static final String lockName = "alock";

    DistributedLock distributedLock;

    @BeforeEach
    public void createDistributedLock() {
        distributedLock = new DistributedLockBuilder(CLUSTER.bootstrapServers()).distributedLock(lockName);
    }

    @Test
    public void testLock() throws InterruptedException, ExecutionException {
        Thread t1 = new Thread(() -> {
            System.out.println("T1: Trying to lock");
            distributedLock.lock();
            System.out.println("T1: Acquired lock");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("T1: Completed work");
            distributedLock.unlock();
            System.out.println("T1: Unlocked");
        });
        Thread t2 = new Thread(() -> {
            System.out.println("T2: Trying to lock");
            distributedLock.lock();
            System.out.println("T2: Acquired lock");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("T2: Completed work");
            distributedLock.unlock();
            System.out.println("T2: Unlocked");
        });
        Thread t3 = new Thread(() -> {
            System.out.println("T3: Trying to lock");
            distributedLock.lock();
            System.out.println("T3: Acquired lock");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("T3: Completed work");
            distributedLock.unlock();
            System.out.println("T3: Unlocked");
        });

        t1.start();
        t2.start();
        t3.start();

        Thread.sleep(3000);

        DistributedLock anotherLock = new DistributedLockBuilder(CLUSTER.bootstrapServers()).distributedLock("anotherLock");
        Thread x1 = new Thread(() -> {
            System.out.println("X1: Trying to lock");
            anotherLock.lock();
            System.out.println("X1: Acquired lock");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("X1: Completed work");
            anotherLock.unlock();
            System.out.println("X1: Unlocked");
        });
        x1.start();
        Thread x2 = new Thread(() -> {
            System.out.println("X2: Trying to lock");
            anotherLock.lock();
            System.out.println("X2: Acquired lock");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("X2: Completed work");
            anotherLock.unlock();
            System.out.println("X2: Unlocked");
        });
        x2.start();

        Thread.sleep(4000);
        Thread t4 = new Thread(() -> {
            System.out.println("T4: Trying to lock");
            distributedLock.lock();
            System.out.println("T4: Acquired lock");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("T4: Completed work");
            distributedLock.unlock();
            System.out.println("T4: Unlocked");
        });
        t4.start();

        t1.join();
        t2.join();
        t3.join();
        t4.join();
        x1.join();
        x2.join();
    }
}
