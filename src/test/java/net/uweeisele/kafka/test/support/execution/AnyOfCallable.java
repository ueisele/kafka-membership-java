package net.uweeisele.kafka.test.support.execution;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;

import static java.lang.String.format;

public class AnyOfCallable<T> implements Callable<T> {

    private final Collection<? extends CompletableFuture<T>> futures;

    public AnyOfCallable(Collection<? extends CompletableFuture<T>> futures) {
        this.futures = futures;
    }

    @Override
    public T call() throws InterruptedException {
        BlockingQueue<Pair<T, Throwable>> completionQueue = new LinkedBlockingDeque<>();
        futures.forEach(f -> f.whenComplete((r, t) -> {
            if (t == null) {
                completionQueue.add(ImmutablePair.of(r, null));
            } else {
                completionQueue.add(ImmutablePair.of(null, t));
            }
        }));

        boolean hasResult = false;
        T result = null;
        List<Throwable> exceptions = new ArrayList<>();
        do {
            Pair<T, Throwable> entry = completionQueue.take();
            if (entry.getRight() != null) {
                exceptions.add(entry.getRight());
            } else {
                hasResult = true;
                result = entry.getLeft();
            }
        } while (!hasResult && exceptions.size() < futures.size());

        if (!hasResult && !exceptions.isEmpty()) {
            throw new ExecutionWrapperException(format("All of the %d futures completed exceptionally", futures.size()), exceptions);
        }

        return result;
    }
}
