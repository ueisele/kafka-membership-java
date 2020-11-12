package net.uweeisele.kafka.test.support;

import static org.junit.jupiter.api.Assertions.fail;

public class Assertion {

    public static <O,E extends Exception> void isEmpty(O o, E e) {
        if (o != null) {
            fail("Expected empty, but found element: " + o);
        }
    }

}
