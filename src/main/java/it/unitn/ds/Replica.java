package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
   * Scheduler for scheduling send and check heartbeat messages operations
   */
  private ScheduledExecutorService scheduler;

  /**
   * This attribute will have the scheduled task for sending heartbeat messages
   */
  private ScheduledFuture<?> heartbeatSendTask;

  /**
   * This attribute will have the scheduled task for checking whether a heartbeat
   * message was received
   */
  private ScheduledFuture<?> heartbeatReceivedStatusTask;

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
   * This attribute is used to check whether an election message ack was already
   * received
   */
  private boolean wasElectionAckReceived;

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
    scheduler = Executors.newSingleThreadScheduledExecutor();
    hasElectionStarted = false;
    wasElectionAckReceived = false;
    electionRetries = 0;
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
    // TODO: implement
    return 0;
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {
    log("MAIN HANDLER CRASH: " + how_to_crash.type + " (" + how_to_crash.after_n_messages_of_type + ")");

    switch (how_to_crash.type) {
      case Crash.Type.Heartbeat:
        // If the the heartbeatSendTask was present, wait for its completion and cancel
        // it
        if (heartbeatSendTask != null) {
          heartbeatSendTask.cancel(false);
          heartbeatSendTask = null;
        }
        break;
      case Crash.Type.Now:
        if (heartbeatSendTask != null) {
          heartbeatSendTask.cancel(false);
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
      heartbeatSendTask = scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, super.getCoordinatorBeatInterval(),
          TimeUnit.MILLISECONDS);
    } else {
      // Otherwise, start checking for heartbeat messages. scheduleWithFixedDelay wait
      // for the previous execution to end. A 50 ms delay is inserted before the
      // scheduling to start to compensate eventual delay in the initialization
      // process
      heartbeatReceivedStatusTask = scheduler.scheduleWithFixedDelay(this::checkHeartbeatMsgStatus, 50,
          getCoordinatorBeatInterval() + super.getMaxLatency(), TimeUnit.MILLISECONDS);
    }
  }

  private void sendHeartbeat() {
    for (Integer replicaId : replicasGroup.keySet()) {
      if (replicaId != coordinatorId) {
        ActorRef replica = replicasGroup.get(replicaId);
        Heartbeat heartbeatMsg = new Heartbeat(coordinatorId);

        // Send heartbeat to the replica
        replica.tell(heartbeatMsg, this.getSelf());
      }
    }
  }

  private void checkHeartbeatMsgStatus() {
    if (wasHeartbeatReceived) {
      // If the heartbeat was received, set the variable to false
      wasHeartbeatReceived = false;
    } else {
      super.log("heartbeat not received");
      // Otherwise stop the check and start the leader election
      heartbeatReceivedStatusTask.cancel(true);

      // Reset all election variables
      hasElectionStarted = false;
      wasElectionAckReceived = false;
      electionRetries = 0;

      // Await a random time before sending the election message. This can potentially
      // avoid multiple election messages
      Random randomDelay = new Random();
      scheduler.schedule(() -> {
        sendElectionMsg((id + 1) % replicasGroup.size(), Optional.empty());
      }, randomDelay.nextInt(super.getMaxLatency() + 1000),
          TimeUnit.MILLISECONDS);
    }
  }

  private void sendElectionMsg(int destinationReplicaId, Optional<Map<Integer, Integer>> receivedPreviousReplicasMap) {
    if (!hasElectionStarted) {
      // Callback call (invoked when it is the first one to detect the crash)
      super.callbackOnElectionStarted(coordinatorId, null);
    }
    if (!hasElectionStarted || !wasElectionAckReceived) {
      // Set the election start to true
      hasElectionStarted = true;

      // Create the list of previous replicas
      Map<Integer, Integer> previousReplicaList = new HashMap<Integer, Integer>(
          receivedPreviousReplicasMap.orElse(Collections.emptyMap()));

      previousReplicaList.put(id, 0); // TODO add actual current transactionID

      previousReplicaList = Collections.unmodifiableMap(previousReplicaList);

      // Create and send ElectionStarted message
      final ElectionStarted msg = new ElectionStarted(id, coordinatorId, previousReplicaList);

      replicasGroup.get((destinationReplicaId) % replicasGroup.size()).tell(msg, this.getSelf());

      // Wait MAX_LATENCY (+10 ms to count for the next replica receive function to
      // complete) before checking if ack for the election was received

      scheduler.schedule(() -> {
        checkElectionAckReception(receivedPreviousReplicasMap);
      }, AbstractReplica.MAX_LATENCY + 10, TimeUnit.MILLISECONDS);

    }
  }

  private void sendSynchronizationMessage() {
    // Create synchronization message
    // TODO change with actual current transactionId
    CoordinatorElected msg = new CoordinatorElected(this.id, this.id, 0, 0);

    // Create a copy of all the replicas without the current one
    Map<Integer, ActorRef> replicasGroupClone = new HashMap<Integer, ActorRef>();
    replicasGroupClone.putAll(replicasGroup);

    replicasGroupClone.remove(this.id);

    Collection<ActorRef> replicas = replicasGroupClone.values();

    // Restart sending heartbeats
    heartbeatSendTask = scheduler.scheduleAtFixedRate(this::sendHeartbeat, 0, super.getCoordinatorBeatInterval(),
        TimeUnit.MILLISECONDS);

    // Send message in broadcast
    for (ActorRef replica : replicas) {
      replica.tell(msg, this.getSelf());
    }

    super.callbackOnCoordinatorElected(this.id, 0, 0);
    hasElectionStarted = false;
    wasElectionAckReceived = false;
    electionRetries = 0;
  }

  private void checkElectionAckReception(Optional<Map<Integer, Integer>> receivedPreviousReplicasMap) {
    if (hasElectionStarted && !wasElectionAckReceived) {
      // If election was started but no ack was received, retry
      electionRetries += 1;

      int newReplicaId = (id + electionRetries) % replicasGroup.size();

      // Check if the previous replica, who failed to receive the message, was in the
      // list. In that case, remove the replica from the message by cloning the
      // existing map
      Map<Integer, Integer> receivedPreviousReplicasMapClone = new HashMap<Integer, Integer>();

      receivedPreviousReplicasMapClone.putAll(receivedPreviousReplicasMap.orElse(Collections.emptyMap()));

      Integer replicaToBeRemovedId = (newReplicaId - 1) % replicasGroup.size();
      receivedPreviousReplicasMapClone.remove(replicaToBeRemovedId);

      receivedPreviousReplicasMapClone = Collections.unmodifiableMap(receivedPreviousReplicasMapClone);

      sendElectionMsg(newReplicaId, Optional.of(receivedPreviousReplicasMapClone));
    } else if (hasElectionStarted) {

      // Sending of election message successful, reset some of the election parameters
      electionRetries = 0;
      wasElectionAckReceived = true;
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

  private void onElectionStartedMsg(ElectionStarted msg) {

    if (!hasElectionStarted) {
      // Invoke call back since election was not started but a message was received
      super.callbackOnElectionStarted(msg.crashedCoordinatorId, null);
      heartbeatReceivedStatusTask.cancel(true);
      wasHeartbeatReceived = false;
      electionRetries = 0;
    }

    // Upon receiving an election message, resend the message to the next replica
    hasElectionStarted = true;

    if (msg.previousReplicasMap.get(id) != null) {
      // If the received list already has the id of this replica, the ring was
      // complete and it's time to select the new leader.

      Integer electedLeader = selectNewLeader(msg.previousReplicasMap);

      // It is possible that multiple election messages are traveling: in that case,
      // don't propagate the message if the to-be leader is the same as the one
      // already selected

      if (electedLeader == coordinatorId) {
        return;
      }

      super.log("new leader " + electedLeader);
      coordinatorId = electedLeader;

      // If the new coordinator is the replica itself, send a synchronization message

      if (coordinatorId == this.id) {
        sendSynchronizationMessage();
        return;
      }

      // Reset ack reception
      wasElectionAckReceived = false;

    }

    // After electing the new leader or have seen the election for the first time,
    // send the message to next entity (sendElectionMsg already filters wrt replica
    // size)
    sendElectionMsg(id + 1, Optional.of(msg.previousReplicasMap));

    // Send election ack message
    ElectionAck ackMsg = new ElectionAck(id);

    replicasGroup.get(msg.replicaId).tell(ackMsg, this.getSelf());
  }

  private void onElectionAckMsg(ElectionAck msg) {
    wasElectionAckReceived = true;
  }

  private void onHeartbeatMsg(Heartbeat msg) {
    if (msg.coordinatorId == coordinatorId) {
      wasHeartbeatReceived = true;
    }
  }

  private void onCoordinatorElectedMsg(CoordinatorElected msg) {
    // Set the coordinator id
    coordinatorId = msg.newCoordinatorId;

    super.callbackOnCoordinatorElected(coordinatorId, msg.transactionId, msg.transactionValue);

    // TODO update transactionId and transactionValue

    // Restart listening for heartbeats
    heartbeatReceivedStatusTask = scheduler.scheduleWithFixedDelay(this::checkHeartbeatMsgStatus, 0,
        getCoordinatorBeatInterval() + super.getMaxLatency(), TimeUnit.MILLISECONDS);

    // Reset hasElectionStarted
    hasElectionStarted = false;
    wasElectionAckReceived = false;
    electionRetries = 0;
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // Listener should be one replica and should be invoked to log
        // coordElect / write / join coordination election / crash message received
        // TODO add your message handlers here .match(, )
        .match(CoordinatorElected.class, this::onCoordinatorElectedMsg)
        .match(ElectionStarted.class, this::onElectionStartedMsg)
        .match(ElectionAck.class, this::onElectionAckMsg)
        .match(Heartbeat.class, this::onHeartbeatMsg)
        .build();
  }
}
