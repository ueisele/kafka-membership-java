package net.uweeisele.kafka.test.support.execution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static java.util.Collections.singleton;
import static java.util.concurrent.Executors.callable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CompletableFutureExecutorService<E extends Executor & Stoppable> implements ExecutorService, Executor, Stoppable {

    private static Logger LOG = LoggerFactory.getLogger(CompletableFutureExecutorService.class);

    private final E executor;

    public CompletableFutureExecutorService(E executor) {
        this.executor = executor;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return new ArrayList<>(invokeAllOf(tasks));
    }

    public <T> List<CompletableFuture<T>> invokeAllOf(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return invokeAllOf(tasks, executor);
    }

    public static <T> List<CompletableFuture<T>> invokeAllOf(Collection<? extends Callable<T>> tasks, Executor executor) throws InterruptedException {
        return invokeAllOf(tasks, Long.MAX_VALUE, MILLISECONDS, executor);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return new ArrayList<>(invokeAllOf(tasks, timeout, unit));
    }

    public <T> List<CompletableFuture<T>> invokeAllOf(Collection<? extends Callable<T>> tasks, Duration timeout) throws InterruptedException {
        return invokeAllOf(tasks, timeout.toMillis(), MILLISECONDS);
    }

    public <T> List<CompletableFuture<T>> invokeAllOf(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return invokeAllOf(tasks, timeout, unit, executor);
    }

    public static <T> List<CompletableFuture<T>> invokeAll(Collection<? extends Callable<T>> tasks, Duration timeout, Executor executor) throws InterruptedException {
        return invokeAllOf(tasks, timeout.toMillis(), MILLISECONDS, executor);
    }

    public static <T> List<CompletableFuture<T>> invokeAllOf(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit, Executor executor) throws InterruptedException {
        List<CompletableFuture<T>> futures = new ArrayList<>();
        CountDownLatch completionJoin = new CountDownLatch(tasks.size());
        try {
            for(Callable<T> task : tasks) {
                futures.add(
                        submit(task, executor).whenComplete((r, e) -> completionJoin.countDown())
                );
            }
            completionJoin.await(timeout, unit);
            return futures;
        } finally {
            cancelAll(futures, true);
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return invokeAny(tasks, executor);
    }

    public static <T> T invokeAny(Collection<? extends Callable<T>> tasks, Executor executor) throws InterruptedException, ExecutionException {
        try {
            return invokeAny(tasks, Long.MAX_VALUE, MILLISECONDS, executor);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Unexpected timeout exception thrown. Timeout was set to nearly infinite.", e);
        }
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, Duration timeout) throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(tasks, timeout.toMillis(), MILLISECONDS);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(tasks, timeout, unit, executor);
    }

    public static <T> T invokeAny(Collection<? extends Callable<T>> tasks, Duration timeout, Executor executor) throws InterruptedException, ExecutionException, TimeoutException {
        return invokeAny(tasks, timeout.toMillis(), MILLISECONDS, executor);
    }

    public static <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit, Executor executor) throws InterruptedException, ExecutionException, TimeoutException {
        List<CompletableFuture<T>> futures = new ArrayList<>();
        CompletableFuture<T> anyOfFuture = null;
        try {
            for(Callable<T> task : tasks) {
                futures.add(submit(task, executor));
            }
            anyOfFuture = submit(new AnyOfCallable<>(futures), ForkJoinPool.commonPool());
            return anyOfFuture.get(timeout, unit);
        } finally {
            cancelAll(futures, true);
            cancelAll(singleton(anyOfFuture), true);
        }
    }

    @Override
    public CompletableFuture<?> submit(Runnable task) {
        return submit(task, executor);
    }

    public static CompletableFuture<?> submit(Runnable task, Executor executor) {
        return submit(callable(task), executor);
    }

    @Override
    public <T> CompletableFuture<T> submit(Runnable task, T result) {
        return submit(task, result, executor);
    }

    public static <T> CompletableFuture<T> submit(Runnable task, T result, Executor executor) {
        return submit(callable(task, result), executor);
    }

    @Override
    public <T> CompletableFuture<T> submit(Callable<T> task) {
        return submit(task, executor);
    }

    public static <T> CompletableFuture<T> submit(Callable<T> task, Executor executor) {
        CompletableFuture<T> future = new CompletableFuture<>();
        CompletableFutureTask<T> ftask = new CompletableFutureTask<>(task, future);
        executor.execute(ftask);
        return future;
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    public static void cancelAll(Collection<? extends Future<?>> futures, boolean mayInterruptIfRunning) {
        futures.stream().filter(Objects::nonNull).forEach(f -> {
            if (!f.isDone()) f.cancel(mayInterruptIfRunning);
        });
    }

    @Override
    public void shutdown() {
        executor.shutdown();
    }

    @Override
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
