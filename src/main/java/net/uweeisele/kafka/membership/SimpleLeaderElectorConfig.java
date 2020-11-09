package net.uweeisele.kafka.membership;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * o.a.k AbstractConfig that parses configs that all Kafka clients require.
 */
class SimpleLeaderElectorConfig extends AbstractConfig {

  private static final ConfigDef CONFIG;

  /**
   * <code>group.id</code>
   */
  public static final String GROUP_ID_CONFIG = "group.id";
  private static final String GROUP_ID_DOC = "A unique string that identifies the member group this member belongs to.";

  public static final long METADATA_MAX_AGE_DEFAULT = 5 * 60 * 1000;
  public static final int SEND_BUFFER_DEFAULT = 128 * 1024;
  public static final int RECEIVE_BUFFER_DEFAULT = 64 * 1024;
  public static final long RECONNECT_BACKOFF_MS_DEFAULT = 50L;
  public static final long RECONNECT_BACKOFF_MAX_MS_DEFAULT = 1000L;
  public static final long RETRY_BACKOFF_MS_DEFAULT = 100L;
  public static final int REQUEST_TIMEOUT_MS_DEFAULT = 305000;
  public static final long CONNECTIONS_MAX_IDLE_MS_DEFAULT = 9 * 60 * 1000;

  static {
    CONFIG = new ConfigDef()
        .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.HIGH,
                CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
        .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG,
                ConfigDef.Type.STRING,
                ClientDnsLookup.DEFAULT.toString(),
                in(ClientDnsLookup.DEFAULT.toString(),
                   ClientDnsLookup.USE_ALL_DNS_IPS.toString(),
                   ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
        .define(GROUP_ID_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                GROUP_ID_DOC)
        .define(CommonClientConfigs.METADATA_MAX_AGE_CONFIG,
                ConfigDef.Type.LONG,
                METADATA_MAX_AGE_DEFAULT,
                atLeast(0),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.METADATA_MAX_AGE_DOC)
        .define(CommonClientConfigs.SEND_BUFFER_CONFIG,
                ConfigDef.Type.INT,
                SEND_BUFFER_DEFAULT,
                atLeast(-1),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SEND_BUFFER_DOC)
        .define(CommonClientConfigs.RECEIVE_BUFFER_CONFIG,
                ConfigDef.Type.INT,
                RECEIVE_BUFFER_DEFAULT,
                atLeast(-1),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.RECEIVE_BUFFER_DOC)
        .define(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                RECONNECT_BACKOFF_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MS_DOC)
        .define(CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_CONFIG,
                ConfigDef.Type.LONG,
                RECONNECT_BACKOFF_MAX_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RECONNECT_BACKOFF_MAX_MS_DOC)
        .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG,
                ConfigDef.Type.LONG,
                RETRY_BACKOFF_MS_DEFAULT,
                atLeast(0L),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
        .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                // chosen to be higher than the default of max.poll.interval.ms
                REQUEST_TIMEOUT_MS_DEFAULT,
                atLeast(0),
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
        .define(CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_CONFIG,
                ConfigDef.Type.LONG,
                CONNECTIONS_MAX_IDLE_MS_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.CONNECTIONS_MAX_IDLE_MS_DOC)
        .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                ConfigDef.Type.STRING,
                CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL,
                ConfigDef.Importance.MEDIUM,
                CommonClientConfigs.SECURITY_PROTOCOL_DOC)
        .withClientSslSupport()
        .withClientSaslSupport()
        .define(CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG,
                ConfigDef.Type.LONG,
                30000,
                atLeast(0),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC)
        .define(CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG,
                ConfigDef.Type.INT,
                2,
                atLeast(1),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.METRICS_NUM_SAMPLES_DOC)
        .define(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG,
                ConfigDef.Type.LIST,
                Collections.emptyList(),
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.LOW,
                CommonClientConfigs.METRIC_REPORTER_CLASSES_DOC);

  }

  SimpleLeaderElectorConfig(Map<String, ?> props) {
    super(CONFIG, props);
  }

  SimpleLeaderElectorConfig(Map<String, ?> props, boolean doLog) {
    super(CONFIG, props, doLog);
  }
}
