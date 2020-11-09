/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.uweeisele.kafka.test.client;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.MILLIS;

/**
 * Utility functions to make integration testing more convenient.
 */
public class ConsumeProduceTestUtils {

  private static final int UNLIMITED_MESSAGES = -1;
  public static final long DEFAULT_TIMEOUT = 30 * 1000L;

  /**
   * Returns up to `maxMessages` message-values from the topic.
   *
   * @param topic          Kafka topic to read messages from
   * @param maxMessages    Maximum number of messages to read via the consumer.
   * @param consumerConfig Kafka consumer configuration
   * @return The values retrieved via the consumer.
   */
  public static <K, V> List<V> readValues(final String topic, final int maxMessages, final Map<String, Object> consumerConfig) {
    final List<Pair<K, V>> kvs = readKeyValues(topic, maxMessages, consumerConfig);
    return kvs.stream().map(Pair::getValue).collect(Collectors.toList());
  }

  /**
   * Returns as many messages as possible from the topic until a (currently hardcoded) timeout is
   * reached.
   *
   * @param topic          Kafka topic to read messages from
   * @param consumerConfig Kafka consumer configuration
   * @return The KeyValue elements retrieved via the consumer.
   */
  public static <K, V> List<Pair<K, V>> readKeyValues(final String topic, final Map<String, Object> consumerConfig) {
    return readKeyValues(topic, UNLIMITED_MESSAGES, consumerConfig);
  }

  /**
   * Returns up to `maxMessages` by reading via the provided consumer (the topic(s) to read from are
   * already configured in the consumer).
   *
   * @param topic          Kafka topic to read messages from
   * @param maxMessages    Maximum number of messages to read via the consumer
   * @param consumerConfig Kafka consumer configuration
   * @return The KeyValue elements retrieved via the consumer
   */
  public static <K, V> List<Pair<K, V>> readKeyValues(final String topic, final int maxMessages, final Map<String, Object> consumerConfig) {
    final KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerConfig);
    consumer.subscribe(Collections.singletonList(topic));
    final int pollIntervalMs = 100;
    final int maxTotalPollTimeMs = 2000;
    int totalPollTimeMs = 0;
    final List<Pair<K, V>> consumedValues = new ArrayList<>();
    while (totalPollTimeMs < maxTotalPollTimeMs && continueConsuming(consumedValues.size(), maxMessages)) {
      totalPollTimeMs += pollIntervalMs;
      final ConsumerRecords<K, V> records = consumer.poll(Duration.of(pollIntervalMs, MILLIS));
      for (final ConsumerRecord<K, V> record : records) {
        consumedValues.add(ImmutablePair.of(record.key(), record.value()));
      }
    }
    consumer.close();
    return consumedValues;
  }

  private static boolean continueConsuming(final int messagesConsumed, final int maxMessages) {
    return maxMessages <= 0 || messagesConsumed < maxMessages;
  }

  /**
   * Write a collection of KeyValueWithTimestamp pairs, with explicitly defined timestamps, to Kafka
   * and wait until the writes are acknowledged.
   *
   * @param topic          Kafka topic to write the data records to
   * @param records        Data records to write to Kafka
   * @param producerConfig Kafka producer configuration
   * @param <K>            Key type of the data records
   * @param <V>            Value type of the data records
   */
  public static <K, V> void produceKeyValuesWithTimestampsSynchronously(
      final String topic,
      final Collection<KeyValueWithTimestamp<K, V>> records,
      final Map<String, Object> producerConfig)
      throws ExecutionException, InterruptedException {
    final Producer<K, V> producer = new KafkaProducer<>(producerConfig);
    for (final KeyValueWithTimestamp<K, V> record : records) {
      final Future<RecordMetadata> f = producer.send(
          new ProducerRecord<>(topic, null, record.timestamp, record.getKey(), record.getValue()));
      f.get();
    }
    producer.flush();
    producer.close();
  }

  /**
   * @param topic          Kafka topic to write the data records to
   * @param records        Data records to write to Kafka
   * @param producerConfig Kafka producer configuration
   * @param <K>            Key type of the data records
   * @param <V>            Value type of the data records
   */
  public static <K, V> void produceKeyValuesSynchronously(
      final String topic,
      final Collection<Pair<K, V>> records,
      final  Map<String, Object> producerConfig)
      throws ExecutionException, InterruptedException {
    final Collection<KeyValueWithTimestamp<K, V>> keyedRecordsWithTimestamp =
        records
            .stream()
            .map(record -> new KeyValueWithTimestamp<>(record.getKey(), record.getValue(), System.currentTimeMillis()))
            .collect(Collectors.toList());
    produceKeyValuesWithTimestampsSynchronously(topic, keyedRecordsWithTimestamp, producerConfig);
  }

  public static <V> void produceValuesSynchronously(
          final String topic, final Collection<V> records, final  Map<String, Object> producerConfig)
      throws ExecutionException, InterruptedException {
    final Collection<Pair<Object, V>> keyedRecords =
        records
            .stream()
            .map(record -> ImmutablePair.of(null, record))
            .collect(Collectors.toList());
    produceKeyValuesSynchronously(topic, keyedRecords, producerConfig);
  }

  public static <K, V> List<Pair<K, V>> waitUntilMinKeyValueRecordsReceived(
      final String topic,
      final int expectedNumRecords,
      final Map<String, Object> consumerConfig)
      throws InterruptedException {
    return waitUntilMinKeyValueRecordsReceived(topic, expectedNumRecords, DEFAULT_TIMEOUT, consumerConfig);
  }

  /**
   * Wait until enough data (key-value records) has been consumed.
   *
   * @param topic              Topic to consume from
   * @param expectedNumRecords Minimum number of expected records
   * @param waitTime           Upper bound in waiting time in milliseconds
   * @param consumerConfig     Kafka Consumer configuration
   * @return All the records consumed, or null if no records are consumed
   * @throws AssertionError if the given wait time elapses
   */
  public static <K, V> List<Pair<K, V>> waitUntilMinKeyValueRecordsReceived(final String topic,
                                                                            final int expectedNumRecords,
                                                                            final long waitTime,
                                                                            final Map<String, Object> consumerConfig) throws InterruptedException {
    final List<Pair<K, V>> accumData = new ArrayList<>();
    final long startTime = System.currentTimeMillis();
    while (true) {
      final List<Pair<K, V>> readData = readKeyValues(topic, consumerConfig);
      accumData.addAll(readData);
      if (accumData.size() >= expectedNumRecords)
        return accumData;
      if (System.currentTimeMillis() > startTime + waitTime)
        throw new AssertionError("Expected " + expectedNumRecords +
            " but received only " + accumData.size() +
            " records before timeout " + waitTime + " ms");
      Thread.sleep(Math.min(waitTime, 100L));
    }
  }

  public static <V> List<V> waitUntilMinValuesRecordsReceived(final String topic,
                                                              final int expectedNumRecords,
                                                              final Map<String, Object> consumerConfig) throws InterruptedException {

    return waitUntilMinValuesRecordsReceived(topic, expectedNumRecords, DEFAULT_TIMEOUT, consumerConfig);
  }

  /**
   * Wait until enough data (value records) has been consumed.
   *
   * @param topic              Topic to consume from
   * @param expectedNumRecords Minimum number of expected records
   * @param waitTime           Upper bound in waiting time in milliseconds
   * @param consumerConfig     Kafka Consumer configuration
   * @return All the records consumed, or null if no records are consumed
   * @throws AssertionError if the given wait time elapses
   */
  public static <V> List<V> waitUntilMinValuesRecordsReceived(final String topic,
                                                              final int expectedNumRecords,
                                                              final long waitTime,
                                                              final Map<String, Object> consumerConfig) throws InterruptedException {
    final List<V> accumData = new ArrayList<>();
    final long startTime = System.currentTimeMillis();
    while (true) {
      final List<V> readData = readValues(topic, expectedNumRecords, consumerConfig);
      accumData.addAll(readData);
      if (accumData.size() >= expectedNumRecords)
        return accumData;
      if (System.currentTimeMillis() > startTime + waitTime)
        throw new AssertionError("Expected " + expectedNumRecords +
            " but received only " + accumData.size() +
            " records before timeout " + waitTime + " ms");
      Thread.sleep(Math.min(waitTime, 100L));
    }
  }

  /**
   * Creates a map entry (for use with {@link ConsumeProduceTestUtils#mkMap(Map.Entry[])})
   *
   * @param k   The key
   * @param v   The value
   * @param <K> The key type
   * @param <V> The value type
   * @return An entry
   */
  static <K, V> Map.Entry<K, V> mkEntry(final K k, final V v) {
    return new Map.Entry<K, V>() {
      @Override
      public K getKey() {
        return k;
      }

      @Override
      public V getValue() {
        return v;
      }

      @Override
      public V setValue(final V value) {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Creates a map from a sequence of entries
   *
   * @param entries The entries to map
   * @param <K>     The key type
   * @param <V>     The value type
   * @return A map
   */
  @SafeVarargs
  static <K, V> Map<K, V> mkMap(final Map.Entry<K, V>... entries) {
    final Map<K, V> result = new LinkedHashMap<>();
    for (final Map.Entry<K, V> entry : entries) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}