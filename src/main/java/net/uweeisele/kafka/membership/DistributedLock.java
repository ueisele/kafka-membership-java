package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.membership.exception.LeaderElectionInitializationException;
import net.uweeisele.kafka.membership.exception.LeaderElectionInterruptedException;
import net.uweeisele.kafka.membership.exception.LeaderElectionTimeoutException;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class DistributedLock implements Lock {

    private final Map<String, ?> configs;
    private final LeaderElectorBuilder leaderElectorBuilder;

    private final ThreadLocal<MutablePair<SimpleLeaderElector, AtomicInteger>> leaderElectors = ThreadLocal.withInitial(() -> MutablePair.of(null, new AtomicInteger(0)));

    public DistributedLock(Map<String, ?> configs) {
        this(configs, SimpleLeaderElector::new);
    }

    public DistributedLock(Map<String, ?> configs, LeaderElectorBuilder leaderElectorBuilder) {
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
        tryAcquireLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean tryLock() throws LeaderElectionInitializationException {
        try {
            return tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
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
            CountDownLatch joinedLatch = new CountDownLatch(1);
            SimpleLeaderElector leaderElector = leaderElectorBuilder.buildLeaderElector(configs)
                    .addLeaderElectionListener(new SimpleLeaderElectionListener() {
                        @Override
                        public void onBecomeLeader(String memberId, int generation) {
                            joinedLatch.countDown();
                        }
                    });
            try {
                long startTime = System.currentTimeMillis();
                leaderElector.init(time, unit);
                long durationMs = System.currentTimeMillis() - startTime;
                if (!joinedLatch.await(Math.max(unit.toMillis(time) - durationMs, 0), TimeUnit.MILLISECONDS)) {
                    throw new LeaderElectionTimeoutException("Timed out waiting for acquiring lock to complete");
                }
            } catch (final Throwable e) {
                leaderElector.close();
                throw e;
            }
            lockEntry.setLeft(leaderElector);
        }
        lockEntry.getRight().incrementAndGet();
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
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

}
