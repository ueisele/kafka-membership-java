package net.uweeisele.kafka.test.support.execution;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class FutureExecutorService<E extends Executor & Stoppable> implements ExecutorService, Stoppable {

    private static Logger LOG = LoggerFactory.getLogger(FutureExecutorService.class);

    private final E executor;

    public FutureExecutorService(E executor) {
        this.executor = executor;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return invokeAll(tasks, Long.MAX_VALUE, MILLISECONDS);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        CountDownLatch completionJoin = new CountDownLatch(tasks.size());
        List<Future<T>> futures = tasks.stream()
                .map(task -> submit(() -> {
                    try {
                        return task.call();
                    } finally {
                        completionJoin.countDown();
                    }
                }))
                .collect(toList());
        if(!completionJoin.await(timeout, unit)) {
            futures.forEach(f -> {if (!f.isDone()) f.cancel(true);});
        }
        return futures;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        try {
            return invokeAny(tasks, Long.MAX_VALUE, MILLISECONDS);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Unexpected timeout exception thrown. Timeout was set to nearly infinite.", e);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        BlockingQueue<Pair<T, Throwable>> completionQueue = new LinkedBlockingDeque<>();
        List<Future<T>> futures = tasks.stream()
                .map(task -> submit(() -> {
                    try {
                        T result = task.call();
                        completionQueue.add(ImmutablePair.of(result, null));
                        return result;
                    } catch (final Throwable t){
                        completionQueue.add(ImmutablePair.of(null, t));
                        throw t;
                    }
                }))
                .collect(toList());

        T result = null;
        List<Throwable> exceptions = new ArrayList<>();
        Duration effectiveTimeout = Duration.ofMillis(unit.toMillis(timeout));
        do {
            long start = System.currentTimeMillis();
            Pair<T, Throwable> entry = completionQueue.poll(effectiveTimeout.toMillis(), MILLISECONDS);
            if (entry != null) {
                result = entry.getLeft();
                if (entry.getRight() != null) {
                    exceptions.add(entry.getRight());
                }
            }
            effectiveTimeout = effectiveTimeout.minusMillis(System.currentTimeMillis() - start);
        } while (result == null && exceptions.size() < tasks.size() && effectiveTimeout.toMillis() > 0);

        if (result != null) {
            return result;
        } else if (exceptions.size() >= tasks.size()) {
            throw new ExecutionException(new ExecutionWrapperException(exceptions));
        }
        futures.forEach(f -> {if (!f.isDone()) f.cancel(true);});
        throw new TimeoutException(format("Could not complete any task within %d seconds.", unit.toSeconds(timeout)));
    }

    @Override
    public Future<?> submit(Runnable task) {
        return submit(task, null);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return submit(() -> {
            task.run();
            return result;
        });
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        FutureTask<T> ftask = new FutureTask<>(task);
        execute(ftask);
        return ftask;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    public void shutdown() {
        executor.shutdown();
    }

    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    @Override
    public boolean awaitTermination(Duration timeout) throws InterruptedException {
        return executor.awaitTermination(timeout);
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

}
