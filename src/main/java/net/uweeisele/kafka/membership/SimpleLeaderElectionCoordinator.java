package net.uweeisele.kafka.membership;

import org.apache.kafka.clients.consumer.internals.AbstractCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;

final class SimpleLeaderElectionCoordinator extends AbstractCoordinator implements Closeable {

  public static final String SLE_SUBPROTOCOL_V0 = "v0";

  private final SimpleLeaderElectionListener listener;

  public SimpleLeaderElectionCoordinator(
          LogContext logContext,
          ConsumerNetworkClient client,
          String groupId,
          int rebalanceTimeoutMs,
          int sessionTimeoutMs,
          int heartbeatIntervalMs,
          Metrics metrics,
          String metricGrpPrefix,
          Time time,
          long retryBackoffMs,
          SimpleLeaderElectionListener listener) {
    super(logContext,
          client,
          groupId,
          Optional.empty(),
          rebalanceTimeoutMs,
          sessionTimeoutMs,
          heartbeatIntervalMs,
          metrics,
          metricGrpPrefix,
          time,
          retryBackoffMs,
          true
    );
    this.listener = listener;
  }

  @Override
  public String protocolType() {
    return "net.uweeisele.simpleleaderelection";
  }

  public void poll(long timeout) {
    // poll for io until the timeout expires
    final long start = time.milliseconds();
    long now = start;
    long remaining;

    do {
      if (coordinatorUnknown()) {
        ensureCoordinatorReady(time.timer(Long.MAX_VALUE));
        now = time.milliseconds();
      }

      if (rejoinNeededOrPending()) {
        ensureActiveGroup();
        now = time.milliseconds();
      }

      pollHeartbeat(now);

      long elapsed = now - start;
      remaining = timeout - elapsed;

      // Note that because the network client is shared with the background heartbeat thread,
      // we do not want to block in poll longer than the time to the next heartbeat.
      client.poll(time.timer(Math.min(Math.max(0, remaining), timeToNextHeartbeat(now))));

      now = time.milliseconds();
      elapsed = now - start;
      remaining = timeout - elapsed;
    } while (remaining > 0);
  }

  @Override
  public JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata() {
    return new JoinGroupRequestData.JoinGroupRequestProtocolCollection(
            singleton(new JoinGroupRequestData.JoinGroupRequestProtocol()
                    .setName(SLE_SUBPROTOCOL_V0)).iterator());
  }

  @Override
  protected void onJoinPrepare(int generation, String memberId) {
    listener.onPrepareLeaderElection(memberId, generation);
  }

  @Override
  protected Map<String, ByteBuffer> performAssignment(
      String leaderId,
      String protocol,
      List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata
  ) {
    Map<String, ByteBuffer> groupAssignment = new HashMap<>();
    for (JoinGroupResponseData.JoinGroupResponseMember member : allMemberMetadata) {
      groupAssignment.put(member.memberId(), ByteBuffer.wrap(leaderId.getBytes(UTF_8)));
    }
    return groupAssignment;
  }

  @Override
  protected void onJoinComplete(
          int generation,
          String memberId,
          String protocol,
          ByteBuffer memberAssignment
  ) {
    String leaderId = new String(memberAssignment.array(), UTF_8);
    listener.onLeaderElected(memberId, leaderId, generation);
    if (memberId.equals(leaderId)) {
      listener.onBecomeLeader(memberId, generation);
    } else {
      listener.onBecomeFollower(memberId, leaderId, generation);
    }
  }

  public void close(Duration timeout) {
    super.close(time.timer(timeout));
  }

}
