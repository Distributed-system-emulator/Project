package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractReplica {

  // === ATTRIBUTES ===
  /**
   * Represents the list of replicas.
   */
  private Map<Integer, ActorRef> replicasGroup;

  /**
   * The identifier of the coordinator replica within the group.
   */
  private Integer coordinatorId;

  /**
   * This attribute will have the scheduled task for sending heartbeat messages
   */
  private Cancellable heartbeatSendTask;

  /**
   * This attribute will have the scheduled task for checking whether a heartbeat
   * message was received
   */
  private Cancellable heartbeatReceivedStatusTask;

  /**
   * This attribute will have the scheduled task for checking whether the
   * heartbeat listening was restarted
   */
  private Cancellable heartbeatListeningStatusTask;

  /**
   * This attribute is check every
   * {@link coordinatorBeatInterval}+{@link maxLatency} to see whether an
   * heartbeat was received from the coordinator
   */
  private boolean wasHeartbeatReceived;

  /**
   * This attribute is used to check whether an election message was already
   * received
   */
  private boolean hasElectionStarted;

  /**
   * This attribute is used to check how many retry before a successful sending of
   * an election message
   */
  private Integer electionRetries;

  /**
   * In case of multiple election messages started from different entities, this
   * Map keeps track for every new message if the respective ack was received. An
   * entry is deleted when the ack is received for the second time, meaning the
   * original message performed two rounds of the ring. The list is completely
   * emptied out when a synchronization message is received
   */
  private Map<Integer, Integer> receivedElectionAckMap;

  private Map<Integer, Integer> coordinatorSubstitutionMap;

  // === CONSTRUCTORS ===

  public Replica(int id) {
    this(id,
        AbstractReplica.MIN_LATENCY,
        AbstractReplica.MAX_LATENCY,
        AbstractReplica.COORDINATOR_BEAT_INTERVAL,
        Optional.empty());
  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);

    replicasGroup = new HashMap<Integer, ActorRef>();
    heartbeatSendTask = null;
    heartbeatListeningStatusTask = null;
    hasElectionStarted = false;
    electionRetries = 0;
    receivedElectionAckMap = new HashMap<Integer, Integer>();
    coordinatorSubstitutionMap = new HashMap<Integer, Integer>();
  }

  // === PROPS ===

  public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
      ActorRef listener) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
  }

  // === METHODS ===

  @Override
  public int getSystemNumberOfActors() {
    return replicasGroup.size();
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {
    log("MAIN HANDLER CRASH: " + how_to_crash.type + " (" + how_to_crash.after_n_messages_of_type + ")");

    switch (how_to_crash.type) {
      case Crash.Type.Heartbeat:
        // If the the heartbeatSendTask was present, wait for its completion and cancel
        if (heartbeatSendTask != null) {
          heartbeatSendTask.cancel();
          heartbeatSendTask = null;
        }
        break;
      case Crash.Type.Now:
        if (heartbeatSendTask != null) {
          heartbeatSendTask.cancel();
          heartbeatSendTask = null;
        }
        break;
      default:
        log("Crash: Invalid crash type(" + how_to_crash.type + ")");
    }

    // Kill the replica
    getContext().stop(getSelf());
  }

  @Override
  public void initSystem(InitSystem sysInit) {
    // Initialize the replica group with what is contained on the initialization
    // message

    replicasGroup = Collections.unmodifiableMap(sysInit.group);
    coordinatorId = sysInit.coordinator_id;

    // If the current replica is the coordinator, start sending heartbeat messages

    if (super.id == coordinatorId) {
      // scheduleAtFixedRate schedule without waiting for the previous task to end
      heartbeatSendTask = getContext().system().scheduler().scheduleAtFixedRate(
          Duration.Zero(),
          Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
          getSelf(),
          new SendHeartbeat(),
          getContext().dispatcher(),
          getSelf());

    } else {
      // If the current replica is not the coordinator, normally it starts to listen
      // to heartbeat after receiving the first one. Wait 2 seconds and check if the
      // listening was started, otherwise start it anyway
      heartbeatListeningStatusTask = getContext().system().scheduler().scheduleOnce(
          Duration.create(2000, TimeUnit.MILLISECONDS),
          getSelf(),
          new CheckHeartbeatListeningStatus(),
          getContext().getSystem().dispatcher(),
          getSelf());
    }
  }

  private void sendHeartbeat(SendHeartbeat msg) {
    for (Integer replicaId : replicasGroup.keySet()) {
      if (replicaId != coordinatorId) {
        ActorRef replica = replicasGroup.get(replicaId);
        Heartbeat heartbeatMsg = new Heartbeat(coordinatorId);

        // Send heartbeat to the replica
        this.tell(heartbeatMsg, replica);
      }
    }
  }

  private void checkHeartbeatMsgStatus(CheckHeartbeatMsgStatus msg) {
    if (wasHeartbeatReceived) {
      // If the heartbeat was received, set the variable to false
      wasHeartbeatReceived = false;
    } else {
      // Otherwise stop the check and start the leader election
      heartbeatReceivedStatusTask.cancel();

      // Reset all election variables
      hasElectionStarted = false;
      receivedElectionAckMap.clear();
      electionRetries = 0;

      // Before sending an election message, wait for the worst case: the node id+1
      // already sent one (check is performed upon receiving the scheduled message).
      // This means every node must wait
      // replicasGroup.size*(max_latency+100), where 100 is the estimated time for
      // every node to process the message

      getContext().system().scheduler()
          .scheduleOnce(
              Duration.create(replicasGroup.size() * (getMaxLatency() + 100), TimeUnit.MILLISECONDS),
              getSelf(),
              new SendElectionMsg(this.id, (id + 1) % replicasGroup.size(), Optional.empty()),
              getContext().getSystem().dispatcher(),
              getSelf());

    }
  }

  /**
   * This function send an election message
   * 
   * @param destinationReplicaId
   * @param receivedPreviousReplicasMap
   */
  private void sendElectionMsg(int originalReplicaId, int destinationReplicaId,
      Optional<Map<Integer, Integer>> receivedPreviousReplicasMap) {

    if (!hasElectionStarted) {
      // Callback call (invoked when it is the first one to detect the crash)
      super.callbackOnElectionStarted(coordinatorId, originalReplicaId, null);
    }

    if (!hasElectionStarted || (wasElectionAckReceived(originalReplicaId) == 0)) {
      // Set the election start to true
      hasElectionStarted = true;

      // Block the heartbeat listening task check: an election is ongoing
      if (heartbeatListeningStatusTask != null && !heartbeatListeningStatusTask.isCancelled()) {
        heartbeatListeningStatusTask.cancel();
      }

      // Create the list of previous replicas
      Map<Integer, Integer> previousReplicaList = new HashMap<Integer, Integer>(
          receivedPreviousReplicasMap.orElse(Collections.emptyMap()));

      previousReplicaList.put(id, 0); // TODO add actual current transactionID

      previousReplicaList = Collections.unmodifiableMap(previousReplicaList);

      // Create and send ElectionStarted message. Store the desired ack on the list

      final ElectionStarted msg = new ElectionStarted(originalReplicaId, id, coordinatorId, previousReplicaList);

      receivedElectionAckMap.put(originalReplicaId, 0);

      this.tell(msg, replicasGroup.get((destinationReplicaId) % replicasGroup.size()));

      // Wait MAX_LATENCY*2 (round trip time) before checking if
      // ack for the election was received. Since every node may be buffering other
      // messages, 100 ms are added for every node

      getContext().system().scheduler()
          .scheduleOnce(
              Duration.create(this.getMaxLatency() * 2 + 100 * replicasGroup.size(), TimeUnit.MILLISECONDS),
              getSelf(),
              new CheckElectionAckReception(originalReplicaId, wasElectionAckReceived(originalReplicaId),
                  receivedPreviousReplicasMap),
              getContext().getSystem().dispatcher(),
              getSelf());

    }
  }

  /**
   * Overload of {@code sendElectionMsg} for scheduled operation
   * 
   * @param msg
   */
  private void sendElectionMsg(SendElectionMsg msg) {
    if (!hasElectionStarted) {
      sendElectionMsg(msg.originalReplicaId, msg.destinationReplicaId, msg.receivedPreviousReplicasMap);
    }
  }

  private void sendSynchronizationMessage() {
    coordinatorSubstitutionMap.put(coordinatorId, this.id);

    coordinatorId = this.id;

    // Create synchronization message
    // TODO change with actual current transactionId
    CoordinatorElected msg = new CoordinatorElected(this.id, this.id, 0, 0);

    // Restart sending heartbeats

    heartbeatSendTask = getContext().system().scheduler().scheduleAtFixedRate(
        Duration.Zero(),
        Duration.create(getCoordinatorBeatInterval(), TimeUnit.MILLISECONDS),
        getSelf(),
        new SendHeartbeat(),
        getContext().dispatcher(),
        getSelf());

    // Create a copy of all the replicas without the current one
    Map<Integer, ActorRef> replicasGroupClone = new HashMap<Integer, ActorRef>();
    replicasGroupClone.putAll(replicasGroup);

    replicasGroupClone.remove(this.id);

    Collection<ActorRef> replicas = replicasGroupClone.values();

    // Send message in broadcast
    for (ActorRef replica : replicas) {
      this.tell(msg, replica);
    }

    // Reset for next election
    super.callbackOnCoordinatorElected(this.id, 0, 0);
    hasElectionStarted = false;
    receivedElectionAckMap.clear();
    electionRetries = 0;
  }

  /**
   * Returns -1 if ack was not expected, 1 if the first ack (first round) was
   * received, 2 if a the second ack was received
   * 
   * @param originalReplicaId
   * @return
   */
  private Integer wasElectionAckReceived(int originalReplicaId) {
    Integer result = receivedElectionAckMap.get(originalReplicaId);
    if (result == null) {
      return -1;
    }
    return result;
  }

  private void checkElectionAckReception(CheckElectionAckReception msg) {

    Optional<Map<Integer, Integer>> receivedPreviousReplicasMap = msg.receivedPreviousReplicasMap;
    int originalReplicaId = msg.originalReplicaId;
    Integer ackCounter = wasElectionAckReceived(originalReplicaId);

    if (hasElectionStarted && ackCounter != (msg.previousAckCounter + 1)) {
      // If election was started but no ack was received, retry
      electionRetries += 1;

      int newReplicaId = (id + electionRetries) % replicasGroup.size();

      // Check if the previous replica, who failed to receive the message, was in the
      // list. In that case, remove the replica from the message by cloning the
      // existing map

      Map<Integer, Integer> receivedPreviousReplicasMapClone = new HashMap<Integer, Integer>();

      receivedPreviousReplicasMapClone.putAll(receivedPreviousReplicasMap.orElse(
          Collections.emptyMap()));

      Integer replicaToBeRemovedId = (newReplicaId - 1) % replicasGroup.size();
      receivedPreviousReplicasMapClone.remove(replicaToBeRemovedId);

      receivedPreviousReplicasMapClone = Collections.unmodifiableMap(receivedPreviousReplicasMapClone);

      sendElectionMsg(originalReplicaId, newReplicaId, Optional.of(receivedPreviousReplicasMapClone));
    } else if (hasElectionStarted) {
      // Sending of election message successful, reset some of the election parameters

      Integer value = receivedElectionAckMap.get(originalReplicaId);
      if (value != null && value != 2) {
        receivedElectionAckMap.put(originalReplicaId, value += 1);
      } else if (value == 2) {
        receivedElectionAckMap.remove(originalReplicaId);
      }
    }
  }

  private Integer selectNewLeader(Map<Integer, Integer> replicasMap) {
    // New leader is the one with the highest transaction identifier. In case
    // multiple are present, the one with the highest id is selected

    Set<Integer> replicasId = replicasMap.keySet();
    Integer newLeaderId = -1;
    Integer newLeaderTransactionId = -1;

    for (Integer id : replicasId) {
      Integer transactionId = replicasMap.get(id);

      if (transactionId > newLeaderTransactionId) {

        newLeaderId = id;
        newLeaderTransactionId = transactionId;

      } else if (transactionId == newLeaderTransactionId && id > newLeaderId) {

        newLeaderId = id;

      }
    }

    return newLeaderId;
  }

  private void checkHeartbeatListeningStatus(CheckHeartbeatListeningStatus msg) {
    if (heartbeatReceivedStatusTask == null || heartbeatReceivedStatusTask.isCancelled()) {
      heartbeatReceivedStatusTask = getContext().system().scheduler().scheduleWithFixedDelay(
          Duration.Zero(),
          Duration.create(super.getCoordinatorBeatInterval() + super.getMaxLatencyPlusTolerance(),
              TimeUnit.MILLISECONDS),
          getSelf(),
          new CheckHeartbeatMsgStatus(),
          getContext().dispatcher(),
          getSelf());
    }
  }

  private void onElectionStartedMsg(ElectionStarted msg) {

    // Send election ack message
    ElectionAck ackMsg = new ElectionAck(msg.originalReplicaId, id);
    this.tell(ackMsg, replicasGroup.get(msg.replicaId));

    Integer electedLeader = selectNewLeader(msg.previousReplicasMap);

    // If an election message is received by the coordinator, the coordinator is not
    // dead, therefore the election should be aborted and the message is brought out
    // of the link. The message is removed also if the elected leader is the same as
    // a previous message that was circulating in the ring or if the new leader has
    // already been elected (maybe a message was traveling while the
    // synchronization message was being sent)

    Integer coordinatorSubstitute = coordinatorSubstitutionMap.get(msg.crashedCoordinatorId);

    if (this.id == coordinatorId || electedLeader == coordinatorId
        || (coordinatorSubstitute != null && coordinatorSubstitute >= 0)) {
      return;
    }

    if (!hasElectionStarted) {
      // Invoke call back since election was not started but a message was received
      super.callbackOnElectionStarted(msg.crashedCoordinatorId, msg.originalReplicaId, null);
      if (heartbeatReceivedStatusTask != null) {
        heartbeatReceivedStatusTask.cancel();
      }

      wasHeartbeatReceived = false;
      electionRetries = 0;
    }

    // Upon receiving an election message, resend the message to the next replica
    hasElectionStarted = true;

    if (msg.previousReplicasMap.get(id) != null) {

      // If the new coordinator is the replica itself, send a synchronization message
      if (electedLeader == this.id) {
        sendSynchronizationMessage();
        return;
      }

    }

    // Propagate the message (sendElectionMsg already filters wrt replica
    // size)

    receivedElectionAckMap.put(msg.originalReplicaId, 0);
    sendElectionMsg(msg.originalReplicaId, id + 1, Optional.of(msg.previousReplicasMap));
  }

  private void onElectionAckMsg(ElectionAck msg) {
    electionRetries = 0;
    Integer ackCounter = receivedElectionAckMap.get(msg.originalReplicaId);
    if (ackCounter != null) {
      receivedElectionAckMap.put(msg.originalReplicaId, ackCounter += 1);
    }
  }

  private void onHeartbeatMsg(Heartbeat msg) {
    wasHeartbeatReceived = true;

    if (heartbeatReceivedStatusTask == null || heartbeatReceivedStatusTask.isCancelled()) {
      // Restart the listening if it was not active. This can happen after a leader
      // election
      heartbeatReceivedStatusTask = getContext().system().scheduler().scheduleWithFixedDelay(
          Duration.Zero(),
          Duration.create(super.getCoordinatorBeatInterval() + super.getMaxLatencyPlusTolerance(),
              TimeUnit.MILLISECONDS),
          getSelf(),
          new CheckHeartbeatMsgStatus(),
          getContext().dispatcher(),
          getSelf());
    }
  }

  private void onCoordinatorElectedMsg(CoordinatorElected msg) {
    coordinatorSubstitutionMap.put(coordinatorId, msg.newCoordinatorId);

    // Set the coordinator id
    coordinatorId = msg.newCoordinatorId;

    super.callbackOnCoordinatorElected(coordinatorId, msg.transactionId, msg.transactionValue);

    // TODO update transactionId and transactionValue

    // heartbeats listening will restart upon receiving a new heartbeat from the new
    // leader, however the new coordinator can shutdown before being able to send
    // the message, therefore, check if the task was started after 2 seconds
    heartbeatListeningStatusTask = getContext().system().scheduler().scheduleOnce(
        Duration.create(2000, TimeUnit.MILLISECONDS),
        getSelf(),
        new CheckHeartbeatListeningStatus(),
        getContext().getSystem().dispatcher(),
        getSelf());

    // Reset for next election
    hasElectionStarted = false;
    electionRetries = 0;
    receivedElectionAckMap.clear();
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // Listener should be one replica and should be invoked to log
        // coordElect / write / join coordination election / crash message received
        // TODO add your message handlers here .match(, )
        .match(CheckHeartbeatListeningStatus.class, this::checkHeartbeatListeningStatus)
        .match(SendElectionMsg.class, this::sendElectionMsg)
        .match(CheckElectionAckReception.class, this::checkElectionAckReception)
        .match(CheckHeartbeatMsgStatus.class, this::checkHeartbeatMsgStatus)
        .match(SendHeartbeat.class, this::sendHeartbeat)
        .match(CoordinatorElected.class, this::onCoordinatorElectedMsg)
        .match(ElectionStarted.class, this::onElectionStartedMsg)
        .match(ElectionAck.class, this::onElectionAckMsg)
        .match(Heartbeat.class, this::onHeartbeatMsg)
        .build();
  }
}
