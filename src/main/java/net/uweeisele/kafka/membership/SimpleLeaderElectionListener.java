package net.uweeisele.kafka.membership;

public interface SimpleLeaderElectionListener {

  /**
   * Invoked prior to each leader election. The election is sticky,
   * so if the current leader is still in the group, it will remain the leader.
   * This is typically used to perform any cleanup from the previous generation
   *
   * @param groupId The identifier of the election group
   * @param localMemberId The identifier of this member in the previous group or "" if there was none
   * @param generation The previous generation or -1 if there was none
   */
  default void onPrepareElectionGroupJoin(String groupId, String localMemberId, int generation) {}

  /**
   * Invoked when a group member has successfully joined a group and a leader has been elected.
   * The election is sticky, so if the previous leader is still in the group, it will remain the leader.
   * This method is also called, if the leader did not change.
   *
   * @param groupId The identifier of the election group
   * @param localMemberId The identifier for the local member in the group
   * @param leaderId The identifier of the elected leader in the group
   * @param generation The generation for which the given leader has been elected
   */
  default void onElectionGroupJoined(String groupId, String localMemberId, String leaderId, int generation) {}

  /**
   * Invoked when a group member has successfully joined a group
   * and this group member has been elected as the leader.
   * This method is also called, if the leader did not change.
   *
   * @param groupId The identifier of the election group
   * @param localMemberId The identifier for the local member in the group as well as the identifier of the elected leader
   * @param generation The generation for which the given leader has been elected
   */
  default void onBecomeLeader(String groupId, String localMemberId, int generation) {}

  /**
   * Invoked when a group member has successfully joined a group
   * and this group member has not been elected as the leader.
   * This method is also called, if the leader did not change.
   *
   * @param groupId The identifier of the election group
   * @param localMemberId The identifier for the local member in the group
   * @param leaderId The identifier of the elected leader in the group
   * @param generation The generation for which the given leader has been elected
   */
  default void onBecomeFollower(String groupId, String localMemberId, String leaderId, int generation) {}

}
