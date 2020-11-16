package net.uweeisele.kafka.test.support.execution;

import java.util.Collection;

import static java.util.Collections.unmodifiableCollection;

public class ExecutionWrapperException extends RuntimeException {

    private final Collection<Throwable> exceptions;

    public ExecutionWrapperException(Collection<Throwable> exceptions) {
        this(null, exceptions);
    }

    public ExecutionWrapperException(String message, Collection<Throwable> exceptions) {
        super(message, exceptions.stream().findFirst().orElse(null));
        this.exceptions = unmodifiableCollection(exceptions);
    }

    public Collection<Throwable> getExceptions() {
        return exceptions;
    }

}
