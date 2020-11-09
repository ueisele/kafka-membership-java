package net.uweeisele.kafka.membership;

interface SimpleLeaderElectionListener {

  default void onLeaderElected(String memberId, String leaderId, int generation) {};

  default void onBecomeLeader(String memberId, int generation) {};

  default void onBecomeFollower(String memberId, String leaderId, int generation) {};

}
