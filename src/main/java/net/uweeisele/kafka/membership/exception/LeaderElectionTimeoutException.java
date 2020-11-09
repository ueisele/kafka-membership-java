package net.uweeisele.kafka.membership.exception;

public class LeaderElectionTimeoutException extends LeaderElectionException {

  public LeaderElectionTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public LeaderElectionTimeoutException(String message) {
    super(message);
  }

  public LeaderElectionTimeoutException(Throwable cause) {
    super(cause);
  }

  public LeaderElectionTimeoutException() {
    super();
  }
}
