package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.membership.exception.LeaderElectionInitializationException;
import net.uweeisele.kafka.membership.exception.LeaderElectionInterruptedException;
import net.uweeisele.kafka.membership.exception.LeaderElectionTimeoutException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DistributedLock implements Lock {

    private final Logger LOG = LoggerFactory.getLogger(DistributedLock.class);

    private final Map<String, ?> configs;
    private final SimpleLeaderElectorBuilder leaderElectorBuilder;

    private final ThreadLocal<MutablePair<SimpleLeaderElector, AtomicInteger>> leaderElectors = ThreadLocal.withInitial(() -> MutablePair.of(null, new AtomicInteger(0)));

    public DistributedLock(Map<String, ?> configs) {
        this(configs, SimpleLeaderElector::new);
    }

    public DistributedLock(Map<String, ?> configs, SimpleLeaderElectorBuilder leaderElectorBuilder) {
        this.configs = configs;
        this.leaderElectorBuilder = leaderElectorBuilder;
    }

    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LeaderElectionInterruptedException("Interrupted while waiting for leader election.", e);
        }
    }

    @Override
    public void lockInterruptibly() throws LeaderElectionInitializationException, LeaderElectionTimeoutException, InterruptedException {
        tryAcquireLock(Long.MAX_VALUE, MILLISECONDS);
    }

    @Override
    public boolean tryLock() throws LeaderElectionInitializationException {
        try {
            return tryLock(Long.MAX_VALUE, MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws LeaderElectionInitializationException, InterruptedException {
        try {
            tryAcquireLock(time, unit);
        } catch (LeaderElectionTimeoutException e) {
            return false;
        }
        return true;
    }

    public void tryAcquireLock(long time, TimeUnit unit) throws LeaderElectionInitializationException, LeaderElectionTimeoutException, InterruptedException {
        MutablePair<SimpleLeaderElector, AtomicInteger> lockEntry = leaderElectors.get();
        if (lockEntry.getLeft() == null) {
            SimpleLeaderElector elector = leaderElectorBuilder.build(configs);
            LOG.info("Trying to acquire lock for election group \"{}\"", elector.getGroupId());
            try {
                long startTime = System.currentTimeMillis();
                elector.joinElection();
                if (elector.awaitElectionGroupJoined(time, unit).isEmpty()) {
                    throw new LeaderElectionTimeoutException("Timed out waiting for joining the election");
                }
                long durationMs = System.currentTimeMillis() - startTime;
                if (elector.awaitLeadership(Math.max(unit.toMillis(time) - durationMs, 0), MILLISECONDS).isEmpty()) {
                    throw new LeaderElectionTimeoutException("Timed out waiting for acquiring lock to complete");
                }
            } catch (final Throwable e) {
                LOG.info("Failed to acquire lock for election group \"{}\": {}", elector.getGroupId(), e.getMessage());
                elector.close();
                throw e;
            }
            lockEntry.setLeft(elector);
        }
        lockEntry.getRight().incrementAndGet();
        LOG.info("Acquired lock for election group \"{}\"", lockEntry.getLeft().getGroupId());
    }

    @Override
    public void unlock() {
        MutablePair<SimpleLeaderElector, AtomicInteger> lockEntry = leaderElectors.get();
        if (lockEntry.getLeft() == null) {
            throw new IllegalMonitorStateException("No lock acquired.");
        }
        if (lockEntry.getRight().decrementAndGet() <= 0) {
            lockEntry.getLeft().close();
            leaderElectors.remove();
            LOG.info("Released lock for election group \"{}\"", lockEntry.getLeft().getGroupId());
        }
    }

    public boolean isLocked() {
        return leaderElectors.get().getRight().get() > 0;
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

}
