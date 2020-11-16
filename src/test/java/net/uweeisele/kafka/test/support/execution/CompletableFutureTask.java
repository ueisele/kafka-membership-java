package net.uweeisele.kafka.test.support.execution;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class CompletableFutureTask<T> implements Runnable {

    private final Callable<T> callable;
    private final CompletableFuture<T> future;

    public CompletableFutureTask(Callable<T> callable, CompletableFuture<T> future) {
        this.callable = requireNonNull(callable);
        this.future = requireNonNull(future);
    }

    @Override
    public void run() {
        try {
            future.complete(callable.call());
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
    }
}
