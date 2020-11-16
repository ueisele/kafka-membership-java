package net.uweeisele.kafka.test.support.execution;

@FunctionalInterface
public interface RunnableWithThrows {

    void run() throws Exception;
}
