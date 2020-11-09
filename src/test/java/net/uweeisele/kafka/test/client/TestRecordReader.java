package net.uweeisele.kafka.test.client;

import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class TestRecordReader {

    private final Map<String, Object> defaultConsumerConfigs;

    public TestRecordReader(Map<String, Object> defaultConsumerConfigs) {
        this.defaultConsumerConfigs = new HashMap<>(defaultConsumerConfigs);
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic          Kafka topic to read messages from
     * @param maxMessages    Maximum number of messages to read via the consumer.
     * @return The values retrieved via the consumer.
     */
    public <K, V> List<V> readValues(final String topic, final int maxMessages) {
        return readValues(topic, maxMessages, emptyMap());
    }

    /**
     * Returns up to `maxMessages` message-values from the topic.
     *
     * @param topic                 Kafka topic to read messages from
     * @param maxMessages           Maximum number of messages to read via the consumer.
     * @param extraConsumerConfig   Kafka consumer configuration
     * @return The values retrieved via the consumer.
     */
    public <K, V> List<V> readValues(final String topic, final int maxMessages, final Map<String, Object> extraConsumerConfig) {
        return ConsumeProduceTestUtils.readValues(topic, maxMessages, effectiveConsumerConfigs(extraConsumerConfig));
    }

    /**
     * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is
     * reached.
     *
     * @param topic               Kafka topic to read messages from
     * @return The KeyValue elements retrieved via the consumer.
     */
    public <K, V> List<Pair<K, V>> readKeyValues(final String topic) {
        return readKeyValues(topic, emptyMap());
    }

    /**
     * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is
     * reached.
     *
     * @param topic               Kafka topic to read messages from
     * @param extraConsumerConfig Kafka consumer configuration
     * @return The KeyValue elements retrieved via the consumer.
     */
    public <K, V> List<Pair<K, V>> readKeyValues(final String topic, final Map<String, Object> extraConsumerConfig) {
        return ConsumeProduceTestUtils.readKeyValues(topic, effectiveConsumerConfigs(extraConsumerConfig));
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
     * already configured in the consumer).
     *
     * @param topic               Kafka topic to read messages from
     * @param maxMessages         Maximum number of messages to read via the consumer
     * @return The KeyValue elements retrieved via the consumer
     */
    public <K, V> List<Pair<K, V>> readKeyValues(final String topic, final int maxMessages) {
        return readKeyValues(topic, maxMessages, emptyMap());
    }

    /**
     * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
     * already configured in the consumer).
     *
     * @param topic               Kafka topic to read messages from
     * @param maxMessages         Maximum number of messages to read via the consumer
     * @param extraConsumerConfig Kafka consumer configuration
     * @return The KeyValue elements retrieved via the consumer
     */
    public <K, V> List<Pair<K, V>> readKeyValues(final String topic, final int maxMessages, final Map<String, Object> extraConsumerConfig) {
        return ConsumeProduceTestUtils.readKeyValues(topic, maxMessages, effectiveConsumerConfigs(extraConsumerConfig));
    }

    public <K, V> List<Pair<K, V>> waitUntilMinKeyValueRecordsReceived(
            final String topic,
            final int expectedNumRecords)
            throws InterruptedException {
        return waitUntilMinKeyValueRecordsReceived(topic, expectedNumRecords, emptyMap());
    }

    public <K, V> List<Pair<K, V>> waitUntilMinKeyValueRecordsReceived(
            final String topic,
            final int expectedNumRecords,
            final Map<String, Object> extraConsumerConfig)
            throws InterruptedException {
        return ConsumeProduceTestUtils.waitUntilMinKeyValueRecordsReceived(topic, expectedNumRecords, effectiveConsumerConfigs(extraConsumerConfig));
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param topic                 Topic to consume from
     * @param expectedNumRecords    Minimum number of expected records
     * @param waitTime              Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public <K, V> List<Pair<K, V>> waitUntilMinKeyValueRecordsReceived(final String topic,
                                                                       final int expectedNumRecords,
                                                                       final long waitTime) throws InterruptedException {
        return waitUntilMinKeyValueRecordsReceived(topic, expectedNumRecords, waitTime, emptyMap());
    }

    /**
     * Wait until enough data (key-value records) has been consumed.
     *
     * @param topic                 Topic to consume from
     * @param expectedNumRecords    Minimum number of expected records
     * @param waitTime              Upper bound in waiting time in milliseconds
     * @param extraConsumerConfig   Kafka Consumer configuration
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public <K, V> List<Pair<K, V>> waitUntilMinKeyValueRecordsReceived(final String topic,
                                                                       final int expectedNumRecords,
                                                                       final long waitTime,
                                                                       final Map<String, Object> extraConsumerConfig) throws InterruptedException {
        return ConsumeProduceTestUtils.waitUntilMinKeyValueRecordsReceived(topic, expectedNumRecords, waitTime, effectiveConsumerConfigs(extraConsumerConfig));
    }

    public <V> List<V> waitUntilMinValuesRecordsReceived(final String topic,
                                                         final int expectedNumRecords) throws InterruptedException {

        return waitUntilMinValuesRecordsReceived(topic, expectedNumRecords, emptyMap());
    }

    public <V> List<V> waitUntilMinValuesRecordsReceived(final String topic,
                                                         final int expectedNumRecords,
                                                         final Map<String, Object> extraConsumerConfig) throws InterruptedException {

        return ConsumeProduceTestUtils.waitUntilMinValuesRecordsReceived(topic, expectedNumRecords, effectiveConsumerConfigs(extraConsumerConfig));
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param topic                 Topic to consume from
     * @param expectedNumRecords    Minimum number of expected records
     * @param waitTime              Upper bound in waiting time in milliseconds
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public <V> List<V> waitUntilMinValuesRecordsReceived(final String topic,
                                                         final int expectedNumRecords,
                                                         final long waitTime) throws InterruptedException {
        return waitUntilMinValuesRecordsReceived(topic ,expectedNumRecords, waitTime, emptyMap());
    }

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param topic                 Topic to consume from
     * @param expectedNumRecords    Minimum number of expected records
     * @param waitTime              Upper bound in waiting time in milliseconds
     * @param extraConsumerConfig   Kafka Consumer configuration
     * @return All the records consumed, or null if no records are consumed
     * @throws AssertionError if the given wait time elapses
     */
    public <V> List<V> waitUntilMinValuesRecordsReceived(final String topic,
                                                         final int expectedNumRecords,
                                                         final long waitTime,
                                                         final Map<String, Object> extraConsumerConfig) throws InterruptedException {
        return ConsumeProduceTestUtils.waitUntilMinValuesRecordsReceived(topic ,expectedNumRecords, waitTime, effectiveConsumerConfigs(extraConsumerConfig));
    }

    private Map<String, Object> effectiveConsumerConfigs(Map<String, Object> extraConsumerConfigs) {
        Map<String, Object> effectiveConfigs = new HashMap<>(defaultConsumerConfigs);
        effectiveConfigs.putAll(extraConsumerConfigs);
        return effectiveConfigs;
    }
}
