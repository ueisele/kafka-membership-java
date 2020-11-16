package net.uweeisele.kafka.test.support.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class WorkerExecutor implements Executor, Stoppable, Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerExecutor.class);

    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final CountDownLatch terminationJoin = new CountDownLatch(1);

    private final BlockingQueue<Runnable> workQueue;

    private Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> LOG.error(format("Caught unhandled exception %s: %s", e.getClass().getSimpleName(), e.getMessage()), e);

    public WorkerExecutor() {
        this(new LinkedBlockingDeque<>());
    }

    public WorkerExecutor(BlockingQueue<Runnable> workQueue) {
        this.workQueue = workQueue;
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown()) {
            throw new IllegalStateException("Worker has already been stopped!");
        }
        workQueue.add(command);
    }

    @Override
    public void run() {
        while (true) {
            Runnable next = null;
            try {
                next = workQueue.poll(1, SECONDS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                shutdown.set(true);
            }
            if (next != null) {
                try {
                    next.run();
                } catch (Throwable e) {
                    exceptionHandler.uncaughtException(currentThread(), e);
                }
            } else if (shutdown.get()) {
                break;
            }
        }
        terminationJoin.countDown();
    }

    public void shutdown() {
        shutdown.set(true);
    }

    public List<Runnable> shutdownNow() {
        shutdown.set(true);
        List<Runnable> drainedTasks = new ArrayList<>();
        workQueue.drainTo(drainedTasks);
        return drainedTasks;
    }

    @Override
    public boolean isShutdown() {
        return shutdown.get();
    }

    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return awaitTermination(timeout.toMillis(), MILLISECONDS);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationJoin.await(timeout, unit);
    }

    @Override
    public boolean isTerminated() {
        return terminationJoin.getCount() <= 0;
    }

    public WorkerExecutor setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
        return this;
    }
}
