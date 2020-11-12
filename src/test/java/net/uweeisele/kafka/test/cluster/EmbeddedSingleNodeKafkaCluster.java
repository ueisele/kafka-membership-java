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
package net.uweeisele.kafka.test.cluster;

import kafka.server.KafkaConfig$;
import net.uweeisele.kafka.test.client.TestRecordReader;
import net.uweeisele.kafka.test.client.TestRecordWriter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.*;

import static java.util.Collections.emptyMap;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedSingleNodeKafkaCluster implements BeforeAllCallback, AfterAllCallback {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedSingleNodeKafkaCluster.class);

  private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being selected

  private ZooKeeperEmbedded zookeeper;
  private KafkaEmbedded broker;
  private final Properties brokerConfig;
  private boolean running;

  /**
   * Creates and starts the cluster.
   */
  public EmbeddedSingleNodeKafkaCluster() {
    this(emptyMap());
  }

  public EmbeddedSingleNodeKafkaCluster(final Map<String, String> brokerConfig) {
    this.brokerConfig = new Properties();
    this.brokerConfig.putAll(brokerConfig);
  }

  /**
   * Creates and starts the cluster.
   *
   * @param brokerConfig Additional broker configuration settings.
   */
  public EmbeddedSingleNodeKafkaCluster(final Properties brokerConfig) {
    this.brokerConfig = new Properties();
    this.brokerConfig.putAll(brokerConfig);
  }

  /**
   * Creates and starts the cluster.
   */
  public void start() throws Exception {
    LOG.debug("Initiating embedded Kafka cluster startup");
    LOG.debug("Starting a ZooKeeper instance...");
    zookeeper = new ZooKeeperEmbedded();
    LOG.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

    final Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
    LOG.debug("Starting a Kafka instance on port {} ...",
      effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
    broker = new KafkaEmbedded(effectiveBrokerConfig);
    LOG.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
    broker.brokerList(), broker.zookeeperConnect());

    running = true;
  }

  private Properties effectiveBrokerConfigFrom(final Properties brokerConfig, final ZooKeeperEmbedded zookeeper) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.putAll(brokerConfig);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkSessionTimeoutMsProp(), 30 * 1000);
    effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
    effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectionTimeoutMsProp(), 60 * 1000);
    effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
    effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
    effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicReplicationFactorProp(), (short) 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.OffsetsTopicPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig$.MODULE$.AutoCreateTopicsEnableProp(), true);
    return effectiveConfig;
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    start();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    stop();
  }

  /**
   * Stops the cluster.
   */
  public void stop() {
    LOG.info("Stopping Confluent");
    try {
      if (broker != null) {
        broker.stop();
      }
      try {
        if (zookeeper != null) {
          zookeeper.stop();
        }
      } catch (final IOException fatal) {
        throw new RuntimeException(fatal);
      }
    } finally {
      running = false;
    }
    LOG.info("Confluent Stopped");
  }

  /**
   * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
   * <p>
   * You can use this to tell Kafka Streams applications, Kafka producers, and Kafka consumers (new
   * consumer API) how to connect to this cluster.
   */
  public String bootstrapServers() {
    return broker.brokerList();
  }

  /**
   * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
   * Example: `127.0.0.1:2181`.
   * <p>
   * You can use this to e.g. tell Kafka consumers (old consumer API) how to connect to this
   * cluster.
   */
  public String zookeeperConnect() {
    return zookeeper.connectString();
  }

  /**
   * Creates a AdminClient. Dont forget to close it.
   */
  public AdminClient createAdminClient() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    return AdminClient.create(configs);
  }

  public TestRecordWriter recordWriter() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return new TestRecordWriter(configs);
  }

  public TestRecordReader recordReader() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    return new TestRecordReader(configs);
  }

  /**
   * Creates a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  public void createTopic(final String topic) throws InterruptedException {
    createTopic(topic, 1, (short) 1, emptyMap());
  }

  /**
   * Creates a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  public void createTopic(final String topic, final int partitions, final short replication) throws InterruptedException {
    createTopic(topic, partitions, replication, emptyMap());
  }

  /**
   * Creates a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(final String topic,
                          final int partitions,
                          final short replication,
                          final Map<String, String> topicConfig) throws InterruptedException {
    createTopic(60000L, topic, partitions, replication, topicConfig);
  }

  /**
   * Creates a Kafka topic with the given parameters and blocks until all topics got created.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  public void createTopic(final long timeoutMs,
                          final String topic,
                          final int partitions,
                          final short replication,
                          final Map<String, String> topicConfig) throws InterruptedException {
    broker.createTopic(topic, partitions, replication, topicConfig);

    if (timeoutMs > 0) {
      TestUtils.waitForCondition(new TopicCreatedCondition(topic), timeoutMs, "Topics not created after " + timeoutMs + " milli seconds.");
    }
  }

  /**
   * Deletes multiple topics and blocks until all topics got deleted.
   *
   * @param timeoutMs the max time to wait for the topics to be deleted (does not block if {@code <= 0})
   * @param topics    the name of the topics
   */
  public void deleteTopicsAndWait(final long timeoutMs, final String... topics) throws InterruptedException {
    for (final String topic : topics) {
      try {
        broker.deleteTopic(topic);
      } catch (final UnknownTopicOrPartitionException expected) {
        // indicates (idempotent) success
      }
    }

    if (timeoutMs > 0) {
      TestUtils.waitForCondition(new TopicsDeletedCondition(topics), timeoutMs, "Topics not deleted after " + timeoutMs + " milli seconds.");
    }
  }

  public boolean isRunning() {
    return running;
  }

  private final class TopicsDeletedCondition implements TestCondition {
    final Set<String> deletedTopics = new HashSet<>();

    private TopicsDeletedCondition(final String... topics) {
      Collections.addAll(deletedTopics, topics);
    }

    @Override
    public boolean conditionMet() {
      //TODO once KAFKA-6098 is fixed use AdminClient to verify topics have been deleted
      // final Set<String> allTopicsFromZk = new HashSet<>(
      //        JavaConverters.setAsJavaSet(broker.kafkaServer().zkClient().getAllTopicsInCluster()));
      final Set<String> allTopicsFromZk = new HashSet<>(
          JavaConverters.seqAsJavaList(broker.kafkaServer().zkClient().getAllTopicsInCluster()));

      final Set<String> allTopicsFromBrokerCache = new HashSet<>(
          JavaConverters.seqAsJavaListConverter(broker.kafkaServer().metadataCache().getAllTopics().toSeq()).asJava());

      return !allTopicsFromZk.removeAll(deletedTopics) && !allTopicsFromBrokerCache.removeAll(deletedTopics);
    }
  }

  private final class TopicCreatedCondition implements TestCondition {
    final String createdTopic;

    private TopicCreatedCondition(final String topic) {
      createdTopic = topic;
    }

    @Override
    public boolean conditionMet() {
      //TODO once KAFKA-6098 is fixed use AdminClient to verify topics have been deleted
      return broker.kafkaServer().zkClient().getAllTopicsInCluster().contains(createdTopic) &&
          broker.kafkaServer().metadataCache().contains(createdTopic);
    }
  }

}
