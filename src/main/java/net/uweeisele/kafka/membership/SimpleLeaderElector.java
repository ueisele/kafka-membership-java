package net.uweeisele.kafka.membership;

import net.uweeisele.kafka.membership.exception.LeaderElectionInitializationException;
import net.uweeisele.kafka.membership.exception.LeaderElectionTimeoutException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.kafka.clients.*;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SimpleLeaderElector {

  private static final Logger log = LoggerFactory.getLogger(SimpleLeaderElector.class);

  private static final AtomicInteger SLE_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  private static final String METRICS_PREFIX = "kafka.simple.leader.election";

  private final String clientId;
  private final ConsumerNetworkClient client;
  private final Metrics metrics;
  private final SimpleLeaderElectionCoordinator coordinator;

  private final List<SimpleLeaderElectionListener> leaderElectionListeners = new ArrayList<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final CountDownLatch joinedLatch = new CountDownLatch(1);
  private ExecutorService executor;

  public SimpleLeaderElector(String bootstrapServer, String groupId) throws LeaderElectionInitializationException {
    this(mapOfEntries(
            ImmutablePair.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer),
            ImmutablePair.of(SimpleLeaderElectorConfig.GROUP_ID_CONFIG, groupId)));
  }

  @SafeVarargs
  private static Map<String,String> mapOfEntries(Map.Entry<String,String>... entries) {
    return Arrays.stream(entries).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public SimpleLeaderElector(Map<String, ?> configs) throws LeaderElectionInitializationException {
    this(new SimpleLeaderElectorConfig(configs));
  }

  public SimpleLeaderElector(SimpleLeaderElectorConfig clientConfig) throws LeaderElectionInitializationException {
    try {
      clientId = "sle-" + SLE_CLIENT_ID_SEQUENCE.getAndIncrement();

      Map<String, String> metricsTags = new LinkedHashMap<>();
      metricsTags.put("client-id", clientId);
      long sampleWindowMs = clientConfig.getLong(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG);
      MetricConfig metricConfig = new MetricConfig()
          .samples(clientConfig.getInt(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG))
          .timeWindow(sampleWindowMs, TimeUnit.MILLISECONDS)
          .tags(metricsTags);
      List<MetricsReporter>
          reporters = clientConfig.getConfiguredInstances(
          CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
          MetricsReporter.class
      );
      reporters.add(new JmxReporter(METRICS_PREFIX));

      Time time = Time.SYSTEM;

      this.metrics = new Metrics(metricConfig, reporters, time);
      long retryBackoffMs = clientConfig.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
      String groupId = clientConfig.getString(SimpleLeaderElectorConfig.GROUP_ID_CONFIG);
      LogContext logContext = new LogContext("[Simple leader election clientId=" + clientId + ", groupId="
          + groupId + "] ");
      Metadata metadata = new Metadata(
              retryBackoffMs,
              clientConfig.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
              logContext,
              new ClusterResourceListeners()
      );
      List<String> bootstrapServers
          = clientConfig.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(bootstrapServers,
          clientConfig.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG));
      metadata.bootstrap(addresses, time.milliseconds());

      ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(clientConfig, time);
      long maxIdleMs = clientConfig.getLong(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG);

      NetworkClient netClient = new NetworkClient(
          new Selector(maxIdleMs, metrics, time, METRICS_PREFIX, channelBuilder, logContext),
              metadata,
          clientId,
          100, // a fixed large enough value will suffice
          clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG),
          clientConfig.getLong(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG),
          clientConfig.getInt(CommonClientConfigs.SEND_BUFFER_CONFIG),
          clientConfig.getInt(CommonClientConfigs.RECEIVE_BUFFER_CONFIG),
          clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
          ClientDnsLookup.forConfig(
              clientConfig.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)),
          time,
          true,
          new ApiVersions(),
          logContext);

      this.client = new ConsumerNetworkClient(
          logContext,
          netClient,
              metadata,
          time,
              retryBackoffMs,
          clientConfig.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
          Integer.MAX_VALUE
      );
      this.coordinator = new SimpleLeaderElectionCoordinator(
              logContext,
              this.client,
              groupId,
              300000, // Default MAX_POLL_INTERVAL_MS_CONFIG
              10000, // Default SESSION_TIMEOUT_MS_CONFIG
              3000, // Default HEARTBEAT_INTERVAL_MS_CONFIG
              metrics,
              METRICS_PREFIX,
              time,
              retryBackoffMs,
              new SimpleLeaderElectionListener() {
                @Override
                public void onLeaderElected(String memberId, String leaderId, int generation) {
                  joinedLatch.countDown();
                  leaderElectionListeners.forEach(l -> l.onLeaderElected(memberId, leaderId, generation));
                }

                @Override
                public void onBecomeLeader(String memberId, int generation) {
                  leaderElectionListeners.forEach(l -> l.onBecomeLeader(memberId, generation));
                }

                @Override
                public void onBecomeFollower(String memberId, String leaderId, int generation) {
                  leaderElectionListeners.forEach(l -> l.onBecomeFollower(memberId, leaderId, generation));
                }
              }
      );

      AppInfoParser.registerAppInfo(METRICS_PREFIX, clientId, metrics, time.milliseconds());

      log.debug("Simple leader election group member created");
    } catch (Throwable t) {
      // call close methods if internal objects are already constructed
      // this is to prevent resource leak. see KAFKA-2121
      stop(true);
      // now propagate the exception
      throw new LeaderElectionInitializationException("Failed to construct kafka consumer", t);
    }
  }

  public SimpleLeaderElector addLeaderElectionListener(SimpleLeaderElectionListener leaderElectionListener) {
    if (leaderElectionListener != null) {
      leaderElectionListeners.add(leaderElectionListener);
    }
    return this;
  }

  public void init(long timeout, TimeUnit timeUnit) throws LeaderElectionTimeoutException, InterruptedException {
    log.debug("Initializing leader election group member");

    executor = Executors.newSingleThreadExecutor();
    executor.submit(() -> {
      try {
        while (!stopped.get()) {
          coordinator.poll(Integer.MAX_VALUE);
        }
      } catch (WakeupException e) {
        log.info("The coordinator poll loop has been aborted");
      } catch (Throwable t) {
        // TODO: Track state of leader elector: RUNNING, REBALANCING, DEAD, STOPPED, ...
        log.error("Unexpected exception in leader election group processing thread", t);
      }
    });

    if (!joinedLatch.await(timeout, timeUnit)) {
      throw new LeaderElectionTimeoutException("Timed out waiting for join group to complete");
    }

    log.debug("Group member initialized and joined group");
  }

  public void close() {
    if (stopped.get()) {
      return;
    }
    stop(false);
  }

  private void stop(boolean swallowException) {
    log.trace("Stopping the leader election group member.");

    // Interrupt any outstanding poll calls
    if (client != null) {
      client.wakeup();
    }

    // Wait for processing thread to complete
    if (executor != null) {
      executor.shutdown();
      try {
        executor.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            "Interrupted waiting for leader election group processing thread to exit",
            e
        );
      }
    }

    // Do final cleanup
    AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
    this.stopped.set(true);
    closeQuietly(() -> coordinator.close(Duration.ofSeconds(10)), "coordinator", firstException);
    closeQuietly(metrics, "consumer metrics", firstException);
    closeQuietly(client, "consumer network client", firstException);
    AppInfoParser.unregisterAppInfo(METRICS_PREFIX, clientId, metrics);
    if (firstException.get() != null && !swallowException) {
      throw new KafkaException(
          "Failed to stop the leader election group member",
          firstException.get()
      );
    } else {
      log.debug("The leader election group member has stopped.");
    }
  }

  private static void closeQuietly(AutoCloseable closeable,
                                   String name,
                                   AtomicReference<Throwable> firstException
  ) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Throwable t) {
        firstException.compareAndSet(null, t);
        log.error("Failed to close {} with type {}", name, closeable.getClass().getName(), t);
      }
    }
  }
}
