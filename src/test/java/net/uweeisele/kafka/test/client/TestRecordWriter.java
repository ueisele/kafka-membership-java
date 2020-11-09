package net.uweeisele.kafka.test.client;

import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyMap;

public class TestRecordWriter {

    private final Map<String, Object> defaultProducerConfigs;

    public TestRecordWriter() {
        this(emptyMap());
    }

    public TestRecordWriter(Map<String, Object> defaultProducerConfigs) {
        this.defaultProducerConfigs = new HashMap<>(defaultProducerConfigs);
    }

    /**
     * Write a collection of KeyValueWithTimestamp pairs, with explicitly defined timestamps, to Kafka
     * and wait until the writes are acknowledged.
     *
     * @param topic                 Kafka topic to write the data records to
     * @param records               Data records to write to Kafka
     * @param <K>                   Key type of the data records
     * @param <V>                   Value type of the data records
     */
    public <K, V> void produceKeyValuesWithTimestampsSynchronously(
            final String topic,
            final Collection<KeyValueWithTimestamp<K, V>> records)
            throws ExecutionException, InterruptedException {
        produceKeyValuesWithTimestampsSynchronously(topic, records, emptyMap());
    }

    /**
     * Write a collection of KeyValueWithTimestamp pairs, with explicitly defined timestamps, to Kafka
     * and wait until the writes are acknowledged.
     *
     * @param topic                 Kafka topic to write the data records to
     * @param records               Data records to write to Kafka
     * @param extraProducerConfigs  Kafka producer configuration
     * @param <K>                   Key type of the data records
     * @param <V>                   Value type of the data records
     */
    public <K, V> void produceKeyValuesWithTimestampsSynchronously(
            final String topic,
            final Collection<KeyValueWithTimestamp<K, V>> records,
            final Map<String, Object> extraProducerConfigs)
            throws ExecutionException, InterruptedException {
        ConsumeProduceTestUtils.produceKeyValuesWithTimestampsSynchronously(topic, records, effectiveProducerConfigs(extraProducerConfigs));
    }

    /**
     * @param topic                 Kafka topic to write the data records to
     * @param records               Data records to write to Kafka
     * @param <K>                   Key type of the data records
     * @param <V>                   Value type of the data records
     */
    public <K, V> void produceKeyValuesSynchronously(
            final String topic,
            final Collection<Pair<K, V>> records)
            throws ExecutionException, InterruptedException {
        produceKeyValuesSynchronously(topic, records, emptyMap());
    }

    /**
     * @param topic                 Kafka topic to write the data records to
     * @param records               Data records to write to Kafka
     * @param extraProducerConfigs  Kafka producer configuration
     * @param <K>                   Key type of the data records
     * @param <V>                   Value type of the data records
     */
    public <K, V> void produceKeyValuesSynchronously(
            final String topic,
            final Collection<Pair<K, V>> records,
            final  Map<String, Object> extraProducerConfigs)
            throws ExecutionException, InterruptedException {
        ConsumeProduceTestUtils.produceKeyValuesSynchronously(topic, records, effectiveProducerConfigs(extraProducerConfigs));
    }

    public <V> void produceValuesSynchronously(
            final String topic, final Collection<V> records)
            throws ExecutionException, InterruptedException {
        produceValuesSynchronously(topic, records, emptyMap());
    }

    public <V> void produceValuesSynchronously(
            final String topic, final Collection<V> records, final  Map<String, Object> extraProducerConfigs)
            throws ExecutionException, InterruptedException {
        ConsumeProduceTestUtils.produceValuesSynchronously(topic, records, effectiveProducerConfigs(extraProducerConfigs));
    }

    private Map<String, Object> effectiveProducerConfigs(Map<String, Object> extraProducerConfigs) {
        Map<String, Object> effectiveConfigs = new HashMap<>(defaultProducerConfigs);
        effectiveConfigs.putAll(extraProducerConfigs);
        return effectiveConfigs;
    }
}
