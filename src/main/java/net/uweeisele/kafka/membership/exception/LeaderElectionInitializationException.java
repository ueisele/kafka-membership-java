package net.uweeisele.kafka.membership.exception;

public class LeaderElectionInitializationException extends LeaderElectionException {

  public LeaderElectionInitializationException(String message, Throwable cause) {
    super(message, cause);
  }

  public LeaderElectionInitializationException(String message) {
    super(message);
  }

  public LeaderElectionInitializationException(Throwable cause) {
    super(cause);
  }

  public LeaderElectionInitializationException() {
    super();
  }
}
