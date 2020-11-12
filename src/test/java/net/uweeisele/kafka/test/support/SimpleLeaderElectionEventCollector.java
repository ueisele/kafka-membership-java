package net.uweeisele.kafka.test.support;

import net.uweeisele.kafka.membership.ElectionGroupGeneration;
import net.uweeisele.kafka.membership.SimpleLeaderElectionListener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SimpleLeaderElectionEventCollector implements SimpleLeaderElectionListener {

    private final Duration defaultTimeout;

    private final BlockingQueue<ElectionGroupGeneration> generationEvents;

    public SimpleLeaderElectionEventCollector() {
        this(Duration.ofSeconds(15));
    }

    public SimpleLeaderElectionEventCollector(Duration defaultTimeout) {
        this(defaultTimeout, new LinkedBlockingDeque<>());
    }

    public SimpleLeaderElectionEventCollector(Duration defaultTimeout, BlockingQueue<ElectionGroupGeneration> generationEvents) {
        this.defaultTimeout = defaultTimeout;
        this.generationEvents = generationEvents;
    }

    @Override
    public void onElectionGroupJoined(String groupId, String localMemberId, String leaderId, int generation) {
        generationEvents.add(new ElectionGroupGeneration(groupId,localMemberId, leaderId, generation));
    }

    public ElectionGroupGeneration poll() throws TimeoutException, InterruptedException {
        return poll(defaultTimeout);
    }

    public ElectionGroupGeneration poll(Duration timeout) throws TimeoutException, InterruptedException {
        ElectionGroupGeneration value = generationEvents.poll(timeout.toMillis(), MILLISECONDS);
        if (value == null) {
            throw new TimeoutException("Could not poll event for " + timeout.toSeconds() + " seconds.");
        }
        return value;
    }

    public ElectionGroupGeneration poll(int atLeastGen) throws TimeoutException, InterruptedException {
        return poll(atLeastGen, defaultTimeout);
    }

    public ElectionGroupGeneration poll(int atLeastGen, Duration timeout) throws TimeoutException, InterruptedException {
        ElectionGroupGeneration generation;
        Duration effectiveTimeout = timeout;
        do {
            long start = System.currentTimeMillis();
            generation = poll(effectiveTimeout);
            effectiveTimeout = timeout.minusMillis(System.currentTimeMillis() - start);
        } while (generation.getGeneration() < atLeastGen);
        return generation;
    }

    public ElectionGroupGeneration peek() {
        return generationEvents.peek();
    }

    public boolean isEmpty() {
        return peek() == null;
    }

    public int size() {
        return generationEvents.size();
    }

    public List<ElectionGroupGeneration> drainAll() {
        List<ElectionGroupGeneration> generations = new ArrayList<>();
        generationEvents.drainTo(generations);
        return generations;
    }

    public SimpleLeaderElectionEventCollector poll(Consumer<ElectionGroupGeneration> generationConsumer)
            throws TimeoutException, InterruptedException {
        return poll(generationConsumer, defaultTimeout);
    }

    public SimpleLeaderElectionEventCollector poll(Consumer<ElectionGroupGeneration> generationConsumer, Duration timeout)
            throws TimeoutException, InterruptedException {
        generationConsumer.accept(poll(timeout));
        return this;
    }

    public SimpleLeaderElectionEventCollector poll(int atLeastGen, Consumer<ElectionGroupGeneration> generationConsumer)
            throws TimeoutException, InterruptedException {
        return poll(atLeastGen, generationConsumer, defaultTimeout);
    }

    public SimpleLeaderElectionEventCollector poll(int atLeastGen, Consumer<ElectionGroupGeneration> generationConsumer, Duration timeout)
            throws TimeoutException, InterruptedException {
        generationConsumer.accept(poll(atLeastGen, timeout));
        return this;
    }

    public SimpleLeaderElectionEventCollector poll(BiConsumer<ElectionGroupGeneration, TimeoutException> generationConsumer)
            throws InterruptedException {
        return poll(generationConsumer, defaultTimeout);
    }

    public SimpleLeaderElectionEventCollector poll(BiConsumer<ElectionGroupGeneration, TimeoutException> generationConsumer, Duration timeout)
            throws InterruptedException {
        try {
            generationConsumer.accept(poll(timeout), null);
        } catch (TimeoutException e) {
            generationConsumer.accept(null, e);
        }
        return this;
    }

}
