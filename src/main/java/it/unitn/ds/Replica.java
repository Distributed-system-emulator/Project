package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Replica extends AbstractReplica {

  // === ATTRIBUTES ===

  /**
   * Number of the current epoch (corresponds to e in <e,i>).
   */
  private Integer epochNumber;

  /**
   * Number of the timestamp in the current epoch (corresponds to i in <e,i>).
   */
  private Integer epochTimestamp;

  /**
   * Represents the list of replicas.
   */
  private Map<Integer, ActorRef> replicasGroup;

  /**
   * The identifier of the coordinator replica within the group.
   */
  private Integer coordinatorId;

  /**
   * This attribute will have the scheduled task for sending heartbeat messages.
   */
  private Cancellable heartbeatSendTask;

  /**
   * This attribute will have the scheduled task for checking whether a heartbeat
   * message was received.
   */
  private Cancellable heartbeatReceivedStatusTask;

  /**
   * This attribute will have the scheduled task for checking whether the
   * synchronization message arrived after an election.
   */
  private Cancellable checkSynchronizationMsgTask;

  /**
   * This attribute will have the scheduled task for checking whether the
   * heartbeat listening was restarted.
   */
  private Cancellable heartbeatListeningStatusTask;

  /**
   * This attribute is check every
   * {@link coordinatorBeatInterval}+{@link maxLatency} to see whether an
   * heartbeat was received from the coordinator.
   */
  private boolean wasHeartbeatReceived;

  /**
   * This attribute is used to check whether an election message was already
   * received.
   */
  private boolean hasElectionStarted;

  /**
   * This attribute is used to check how many retry before a successful sending of
   * an election message.
   */
  private Integer electionRetries;

  /**
   * In case of multiple election messages started from different entities, this
   * Map keeps track for every new message if the respective ack was received. An
   * entry is deleted when the ack is received for the second time, meaning the
   * original message performed two rounds of the ring. The list is completely
   * emptied out when a synchronization message is received.
   */
  private Map<Integer, Integer> receivedElectionAckMap;

  private Map<Integer, Integer> coordinatorSubstitutionMap;

  /**
   * Current quorum according to active replicas.
   */
  private int quorum;

  /**
   * Counter of ACKs for the quorum.
   */
  private int writeAckCounter;

  /**
   * The write that is currently being processed by the coordinator.
   */
  private WriteNotification unstableWrite;

  /**
   * Queue of write requests received by the coordinator.
   */
  private Queue<WriteRequestToCoordinator> writeRequestsQueue;
  /**
   * Whether the coordinator is processing a write.
   */
  private boolean writeInProgress = false;

  /**
   * (client, write request) pair record.
   * 
   * @param client       the client that requested the write to the replica
   * @param writeRequest the actual write request
   */
  private record ClientWriteRequestPair(ActorRef client, WriteRequest writeRequest) {
  }

  /**
   * Queue of (client, write request) pairs.
   */
  private Queue<ClientWriteRequestPair> clientWriteRequestQueue;

  /**
   * List of replica IDs that did not sent the write notification ACK yet.
   */
  private List<Integer> pendingAcks;
  /**
   * The round of ACKs.
   */
  private long acksRound = 0;

  /**
   * Timestamp of the last write.
   */
  // private long ts = 0;

  /**
   * Database of (person index, location) pairs.
   */
  private Map<Integer, Integer> database;

  private Crash crashMessage;

  // === CONSTRUCTORS ===

  public Replica(int id) {
    this(id,
        AbstractReplica.MIN_LATENCY,
        AbstractReplica.MAX_LATENCY,
        AbstractReplica.COORDINATOR_BEAT_INTERVAL,
        AbstractReplica.HEARTBEAT_STATUS_CHECK_WAIT_TIME,
        AbstractReplica.SYNC_MESSAGE_WAIT_TIME,
        Optional.empty());
    init();
  }

  public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, int heartbeatStatusCheckWaitTime,
      int syncMessageWaitTime, Optional<ActorRef> listener) {
    super(id, minLatency, maxLatency, coordinatorBeatInterval, heartbeatStatusCheckWaitTime, syncMessageWaitTime,
        listener);
    init();
  }

  private void init() {
    this.replicasGroup = new HashMap<Integer, ActorRef>();
    this.writeRequestsQueue = new LinkedList<WriteRequestToCoordinator>();
    this.clientWriteRequestQueue = new LinkedList<ClientWriteRequestPair>();
    this.pendingAcks = new ArrayList<Integer>();

    this.database = new HashMap<Integer, Integer>();
    this.database.put(0, 10);

    this.replicasGroup = new HashMap<Integer, ActorRef>();
    this.heartbeatSendTask = null;
    this.heartbeatListeningStatusTask = null;
    this.checkSynchronizationMsgTask = null;
    this.hasElectionStarted = false;
    this.electionRetries = 0;
    this.receivedElectionAckMap = new HashMap<Integer, Integer>();
    this.coordinatorSubstitutionMap = new HashMap<Integer, Integer>();

    this.crashMessage = null;
  }

  // === PROPS ===

  public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval,
            AbstractReplica.HEARTBEAT_STATUS_CHECK_WAIT_TIME, AbstractReplica.SYNC_MESSAGE_WAIT_TIME,
            Optional.empty()));
  }

  // Props method for automated tests.
  public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval,
      ActorRef listener) {
    return Props.create(Replica.class,
        () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval,
            AbstractReplica.HEARTBEAT_STATUS_CHECK_WAIT_TIME, AbstractReplica.SYNC_MESSAGE_WAIT_TIME,
            Optional.ofNullable(listener)));
  }

  // === METHODS ===

  private void updateQuorum() {
    this.quorum = Math.floorDiv(getSystemNumberOfActors(), 2) + 1;
  }

  private void processWrite(WriteRequestToCoordinator writeRequestToCoordinator) {
    this.writeInProgress = true;

    WriteNotification writeNotification = new WriteNotification(
        writeRequestToCoordinator.replicaId,
        writeRequestToCoordinator.index,
        writeRequestToCoordinator.value);

    this.writeAckCounter = 1; // Include the coordinator in the quorum
    this.unstableWrite = writeNotification;

    broadcast(writeNotification, false, true);
  }

  private void broadcast(Serializable message, boolean includeMyself, boolean setTimeout) {
    for (int i = 0; i < getSystemNumberOfActors(); i++) {
      if (i == id) {
        if (includeMyself) {
          // If requested to send to myself too, use Akka .tell() function without delays
          getSelf().tell(message, getSelf());
        }

        continue;
      }

      tell(message, this.replicasGroup.get(i));
    }

    if (setTimeout) {
      // Set a timeout for waiting the ACKs of all the other replicas
      // (excluding myself)
      this.pendingAcks.clear();
      this.pendingAcks.addAll(replicasGroup.keySet());
      this.pendingAcks.remove(Integer.valueOf(id));

      acksRound++;
      int timeout = 200;
      scheduleMessage(new WriteNotificationTimeout(acksRound), timeout, getSelf());
    }
  }

  private Cancellable scheduleMessage(Serializable message, int delay, ActorRef dst) {
    return getContext().system().scheduler().scheduleOnce(
        Duration.create(delay, TimeUnit.MILLISECONDS),
        dst,
        message,
        getContext().system().dispatcher(),
        getSelf());
  }

  private Cancellable scheduleMessage(Serializable message, int initialDelay, int repetitionDelay, ActorRef dst) {
    return getContext().system().scheduler().scheduleWithFixedDelay(
        Duration.create(initialDelay, TimeUnit.MILLISECONDS),
        Duration.create(repetitionDelay,
            TimeUnit.MILLISECONDS),
        dst,
        message,
        getContext().dispatcher(),
        getSelf());
  }

  private void performUnstableWrite() {
    this.database.replace(this.unstableWrite.index, this.unstableWrite.value);

    callbackOnUpdateApplied(this.unstableWrite.index, this.unstableWrite.value);
  }

  private void processNextWriteIfAny() {
    if (!this.writeRequestsQueue.isEmpty()) {
      // Process pending writes, if any
      processWrite(this.writeRequestsQueue.poll());

      return;
    }

    this.writeInProgress = false;
  }

  @Override
  public int getSystemNumberOfActors() {
    return replicasGroup.size();
  }

  @Override
  public void crash(AbstractReplica.Crash how_to_crash) {
    log("MAIN HANDLER CRASH: " + how_to_crash.type + " (" + how_to_crash.after_n_messages_of_type + ")");

    if (how_to_crash.type == Crash.Type.Now) {
      crashNow();
      return;
    }

    crashMessage = how_to_crash;

  }

  private void crashNow() {
    if (heartbeatSendTask != null && !heartbeatSendTask.isCancelled()) {
      heartbeatSendTask.cancel();
    }
    if (heartbeatReceivedStatusTask != null && !heartbeatReceivedStatusTask.isCancelled()) {
      heartbeatReceivedStatusTask.cancel();
    }
    if (checkSynchronizationMsgTask != null && !checkSynchronizationMsgTask.isCancelled()) {
      checkSynchronizationMsgTask.cancel();
    }
    if (heartbeatListeningStatusTask != null && !heartbeatListeningStatusTask.isCancelled()) {
      heartbeatListeningStatusTask.cancel();
    }

    // Kill the replica
    getContext().stop(getSelf());
  }

  private boolean willReplicaCrash() {
    if (crashMessage != null) {
      if (crashMessage.after_n_messages_of_type == 0) {
        return true;
      }

      crashMessage = new Crash(crashMessage.type, crashMessage.after_n_messages_of_type - 1);
      return false;
    }
    return false;
  }

  @Override
  public void initSystem(InitSystem sysInit) {
    // Initialize the replica group with what is contained on the initialization
    // message

    replicasGroup = Collections.unmodifiableMap(sysInit.group);
    coordinatorId = sysInit.coordinator_id;

    updateQuorum();

    // If the current replica is the coordinator, start sending heartbeat messages

    if (id == coordinatorId) {
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

      heartbeatListeningStatusTask = scheduleMessage(new CheckHeartbeatListeningStatus(),
          getHeartbeatStatusCheckWaitTime(), getSelf());
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

    if (crashMessage != null && crashMessage.type == Crash.Type.Heartbeat) {
      if (willReplicaCrash()) {
        crashNow();
        return;
      }
    }

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
      scheduleMessage(new SendElectionMsg(this.id, (id + 1) % getSystemNumberOfActors(), Optional.empty()),
          getSystemNumberOfActors() * (getMaxLatency() + 100), getSelf());
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

    if (!hasElectionStarted || (wasElectionAckReceived(originalReplicaId) == 0) || (wasElectionAckReceived(
        originalReplicaId) == 1)) {
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

      this.tell(msg, replicasGroup.get((destinationReplicaId) % getSystemNumberOfActors()));

      // Wait MAX_LATENCY*2 (round trip time) before checking if
      // ack for the election was received. Since every node may be buffering other
      // messages, 100 ms are added for every node
      scheduleMessage(new CheckElectionAckReception(originalReplicaId, wasElectionAckReceived(originalReplicaId),
          receivedPreviousReplicasMap), this.getMaxLatency() * 2 + 100 * getSystemNumberOfActors(), getSelf());

    }
  }

  /**
   * Overload of {@code sendElectionMsg} for scheduled operation.
   * 
   * @param msg
   */
  private void sendElectionMsg(SendElectionMsg msg) {
    if (!hasElectionStarted) {
      sendElectionMsg(msg.originalReplicaId, msg.destinationReplicaId, msg.receivedPreviousReplicasMap);
    }
  }

  private void sendSynchronizationMessage() {

    if (crashMessage != null && crashMessage.type == Crash.Type.ElectionSync) {
      if (willReplicaCrash()) {
        crashNow();
        return;
      }
    }

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

    broadcast(msg, false, false);

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

      int newReplicaId = (id + electionRetries) % getSystemNumberOfActors();

      // Check if the previous replica, who failed to receive the message, was in the
      // list. In that case, remove the replica from the message by cloning the
      // existing map

      Map<Integer, Integer> receivedPreviousReplicasMapClone = new HashMap<Integer, Integer>();

      receivedPreviousReplicasMapClone.putAll(receivedPreviousReplicasMap.orElse(
          Collections.emptyMap()));

      Integer replicaToBeRemovedId = (newReplicaId - 1) % getSystemNumberOfActors();
      receivedPreviousReplicasMapClone.remove(replicaToBeRemovedId);

      receivedPreviousReplicasMapClone = Collections.unmodifiableMap(receivedPreviousReplicasMapClone);

      // If the the ack counter now should be 2 (therefore, this is the second time
      // the election message circulates) but no answer was received, check if the
      // current replica is the new leader
      if (selectNewLeader(receivedPreviousReplicasMapClone) == this.id) {
        sendSynchronizationMessage();
        return;
      }

      sendElectionMsg(originalReplicaId, newReplicaId, Optional.of(receivedPreviousReplicasMapClone));
    } else if (hasElectionStarted) {
      // Sending of election message successful, reset some of the election parameters

      Integer value = receivedElectionAckMap.get(originalReplicaId);
      if (value != null && value == 2) {
        receivedElectionAckMap.remove(originalReplicaId);
      }
    }
  }

  private void checkSynchronizationMsgReception(CheckSynchronizationMsg msg) {
    // If the synchronization message was not received the list of received ack size
    // will be different than 0 and hasElectionStarted will be still set to true

    // In such case, reset the election and restart heartbeat listening
    if (receivedElectionAckMap.size() > 0 || hasElectionStarted) {
      receivedElectionAckMap.clear();
      electionRetries = 0;
      hasElectionStarted = false;

      heartbeatListeningStatusTask = scheduleMessage(new CheckHeartbeatListeningStatus(),
          getHeartbeatStatusCheckWaitTime(), getSelf());
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
      heartbeatReceivedStatusTask = scheduleMessage(new CheckHeartbeatMsgStatus(), 0,
          super.getCoordinatorBeatInterval() + super.getMaxLatencyPlusTolerance(), getSelf());
    }
  }

  private void onElectionStartedMsg(ElectionStarted msg) {

    if (crashMessage != null && crashMessage.type == Crash.Type.Election) {
      if (willReplicaCrash()) {
        crashNow();
        return;
      }
    }

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

      // A new election started, disable check for first heartbeat being received
      if (heartbeatReceivedStatusTask != null && !heartbeatReceivedStatusTask.isCancelled()) {
        heartbeatReceivedStatusTask.cancel();
      }

      // A new election started, disable check for synchronization message being
      // received
      if (checkSynchronizationMsgTask != null && !checkSynchronizationMsgTask.isCancelled()) {
        checkSynchronizationMsgTask.cancel();
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
      } else {
        // Otherwise, set a timer to check if the synchronization message arrived (if
        // not, that means the to-be coordinator received the election message for the
        // second time but crashed before being able to send at least one
        // synchronization message)
        checkSynchronizationMsgTask = scheduleMessage(new CheckSynchronizationMsg(), getSyncMessageWaitTime(),
            getSelf());
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
      heartbeatReceivedStatusTask = scheduleMessage(new CheckHeartbeatMsgStatus(), 0,
          super.getCoordinatorBeatInterval() + super.getMaxLatencyPlusTolerance(), getSelf());
    }
  }

  private void onCoordinatorElectedMsg(CoordinatorElected msg) {
    // A synchronization message arrived, disable check for synchronization message
    // being received
    if (checkSynchronizationMsgTask != null && !checkSynchronizationMsgTask.isCancelled()) {
      checkSynchronizationMsgTask.cancel();
    }

    coordinatorSubstitutionMap.put(coordinatorId, msg.newCoordinatorId);

    // Set the coordinator id
    coordinatorId = msg.newCoordinatorId;

    super.callbackOnCoordinatorElected(coordinatorId, msg.transactionId, msg.transactionValue);

    // TODO update transactionId and transactionValue

    // heartbeats listening will restart upon receiving a new heartbeat from the new
    // leader, however the new coordinator can shutdown before being able to send
    // the message, therefore, check if the task was started after 2 seconds
    heartbeatListeningStatusTask = scheduleMessage(new CheckHeartbeatListeningStatus(),
        getHeartbeatStatusCheckWaitTime(), getSelf());

    // Reset for next election
    hasElectionStarted = false;
    electionRetries = 0;
    receivedElectionAckMap.clear();
  }

  public void onReadRequest(ReadRequest readRequest) {
    if (!database.containsKey(readRequest.index)) {
      AbstractClient.ReadResult readResult = new AbstractClient.ReadResult(
          false,
          readRequest.index,
          0,
          this.id);

      tell(readResult, getSender());

      return;
    }

    AbstractClient.ReadResult readResult = new AbstractClient.ReadResult(
        true,
        readRequest.index,
        this.database.get(readRequest.index),
        this.id);

    tell(readResult, getSender());
  }

  public void onWriteRequest(WriteRequest writeRequest) {
    if (!database.containsKey(writeRequest.index)) {
      // Write failed because index not present in the database
      AbstractClient.WriteResult writeResult = new AbstractClient.WriteResult(
          false,
          writeRequest.index,
          0,
          this.id);

      tell(writeResult, getSender());

      return;
    }

    // Store the pair client-request
    this.clientWriteRequestQueue.add(new ClientWriteRequestPair(getSender(), writeRequest));

    // Forward the request to the coordinator
    WriteRequestToCoordinator writeRequestToCoordinator = new WriteRequestToCoordinator(
        this.id,
        writeRequest.index,
        writeRequest.value);
    tell(writeRequestToCoordinator, this.replicasGroup.get(this.coordinatorId));
  }

  public void onWriteRequestToCoordinator(WriteRequestToCoordinator writeRequestToCoordinator) {
    log("WRITE request (" + writeRequestToCoordinator.index + ", " + writeRequestToCoordinator.value + ")");

    if (this.writeInProgress) {
      // ... store the write in the queue if another one is getting processed ...
      this.writeRequestsQueue.add(writeRequestToCoordinator);
    } else if (!this.writeRequestsQueue.isEmpty()) {
      // ... otherwise, if some writes are pending,
      // push the new one and execute the first one in the queue ...
      this.writeRequestsQueue.add(writeRequestToCoordinator);
      processWrite(this.writeRequestsQueue.poll());
    } else {
      // ... finally, if the queue is empty, process the received request
      processWrite(writeRequestToCoordinator);
    }
  }

  public void onWriteNotification(WriteNotification writeNotification) {
    log("WRITE notification (" + writeNotification.index + ", " + writeNotification.value + ")");

    // Other replicas store the unstable write received by the coordinator
    this.unstableWrite = writeNotification;

    // Send back the ACK with my id included
    WriteNotificationAck ack = new WriteNotificationAck(id);
    tell(ack, this.replicasGroup.get(this.coordinatorId));
  }

  public void onWriteNotificationAck(WriteNotificationAck ack) {
    log("WRITE notification ACK (" + ack.replicaId + ")");

    pendingAcks.remove(Integer.valueOf(ack.replicaId));

    if (pendingAcks.isEmpty()) {
      log("All ACKs received");

      processNextWriteIfAny();

      return;
    }

    this.writeAckCounter++;
    if (this.writeAckCounter == this.quorum) {
      log("WRITE quorum reached");

      // The coordinator immediately increases the writes timestamp
      this.epochTimestamp++;

      // Quorum reached, broadcast WriteOK to all replicas including the coordinator
      // itself so, in case, it can send the result to the client too
      WriteOK writeOK = new WriteOK(this.epochTimestamp, this.unstableWrite.originalReplicaId);
      broadcast(writeOK, true, false);
    }
  }

  public void onWriteNotificationTimeout(WriteNotificationTimeout writeNotificationTimeout) {
    // Ignore stale timeout, a new write request is being processed
    if (writeNotificationTimeout.ackRound != this.acksRound)
      return;

    if (!this.pendingAcks.isEmpty()) {
      // The replicas that did not send the ACKs are considered crashed,
      // so update the replicas group and the quorum
      for (int crashedReplicaId : this.pendingAcks) {
        log("NO ACK from " + replicasGroup.get(crashedReplicaId).path().name() + " (CRASH)");

        this.replicasGroup.remove(crashedReplicaId);
      }

      updateQuorum();

      processNextWriteIfAny();
    }
  }

  public void onWriteOK(WriteOK writeOK) {
    log("WRITE OK");

    performUnstableWrite();

    if (this.id != this.coordinatorId) {
      // Other replicas update the writes timestamp with the received one
      this.epochTimestamp = writeOK.epochTimestamp;
    }

    if (writeOK.originalReplicaId == this.id) {
      // I am the replica to which the client requested the processed write
      AbstractClient.WriteResult writeResult = new AbstractClient.WriteResult(
          true,
          this.unstableWrite.index,
          this.unstableWrite.value,
          this.id);

      tell(writeResult, this.clientWriteRequestQueue.poll().client);
    }
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // Listener should be one replica and should be invoked to log
        // coordElect / write / join coordination election / crash message received
        .match(ReadRequest.class, this::onReadRequest)
        .match(WriteRequest.class, this::onWriteRequest)
        .match(WriteRequestToCoordinator.class, this::onWriteRequestToCoordinator)
        .match(WriteNotification.class, this::onWriteNotification)
        .match(WriteNotificationAck.class, this::onWriteNotificationAck)
        .match(WriteNotificationTimeout.class, this::onWriteNotificationTimeout)
        .match(WriteOK.class, this::onWriteOK)
        .match(CheckSynchronizationMsg.class, this::checkSynchronizationMsgReception)
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
