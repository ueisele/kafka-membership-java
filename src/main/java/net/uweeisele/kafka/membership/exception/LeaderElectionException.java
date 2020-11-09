package net.uweeisele.kafka.membership.exception;

public class LeaderElectionException extends RuntimeException {

  public LeaderElectionException(String message, Throwable cause) {
    super(message, cause);
  }

  public LeaderElectionException(String message) {
    super(message);
  }

  public LeaderElectionException(Throwable cause) {
    super(cause);
  }

  public LeaderElectionException() {
    super();
  }
}
