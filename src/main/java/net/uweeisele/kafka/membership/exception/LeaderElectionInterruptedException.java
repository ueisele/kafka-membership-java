package net.uweeisele.kafka.membership.exception;

public class LeaderElectionInterruptedException extends LeaderElectionException {

  public LeaderElectionInterruptedException(String message, Throwable cause) {
    super(message, cause);
  }

  public LeaderElectionInterruptedException(String message) {
    super(message);
  }

  public LeaderElectionInterruptedException(Throwable cause) {
    super(cause);
  }

  public LeaderElectionInterruptedException() {
    super();
  }
}
