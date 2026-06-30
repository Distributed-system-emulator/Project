package it.unitn.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {
  public static void main(String[] args) {
    System.out.println("========================================");
    System.out.println("START");
    System.out.println("========================================\n");

    final int N_REPLICAS = 7;
    final int N_CLIENTS = 4;
    final int COORDINATOR_ID = 1;
    final ActorSystem system = ActorSystem.create("TestMain");

    Logger.setDestinationStdout();
    Logger.setDebugEnabled(true);

    Map<Integer, ActorRef> replicasGroup = new HashMap<>(N_REPLICAS);
    for (int i = 0; i < N_REPLICAS; i++) {
      replicasGroup.put(i,
          system.actorOf(
              Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY,
                  AbstractReplica.COORDINATOR_BEAT_INTERVAL),
              "Replica_" + i));
    }

    /*
     * //Alternative replica map with a replica logger
     * 
     * final ActorRef logReplica = system.actorOf(Replica.props(-1,
     * AbstractReplica.MIN_LATENCY,
     * AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL));
     * 
     * for (int i = 0; i < N_REPLICAS; i += 1) {
     * replicasGroup.put(i,
     * system.actorOf(
     * Replica.propsWithListener(i, AbstractReplica.MIN_LATENCY,
     * AbstractReplica.MAX_LATENCY,
     * AbstractReplica.COORDINATOR_BEAT_INTERVAL, logReplica),
     * "Replica_" + i));
     * }
     */

    replicasGroup = Collections.unmodifiableMap(replicasGroup);

    // Send init message to all replica
    InitSystem initMsg = new InitSystem(replicasGroup, COORDINATOR_ID);
    for (Map.Entry<Integer, ActorRef> entry : replicasGroup.entrySet()) {
      entry.getValue().tell(initMsg, ActorRef.noSender());
    }

    // TODO: Create your clients
    List<ActorRef> clientsGroup = new ArrayList<ActorRef>();
    for (int i = 0; i < N_CLIENTS; i += 1) {
      ActorRef replica = replicasGroup.get(i % replicasGroup.size());
      clientsGroup.add(system.actorOf(
          Client.props(300, 800,
              Optional.ofNullable(replica)),
          String.valueOf(i)));
    }

    /*
     * //Alternative client with a client logger
     * final ActorRef logClient = system.actorOf(Client.props(200, 300,
     * Optional.ofNullable(replicasGroup.get(0))));
     * for (int i = 0; i < N_CLIENTS; i += 1) {
     * ActorRef replica = replicasGroup.get(i % replicasGroup.size());
     * clientsGroup.add(system.actorOf(Client.propsWithListener(200, 300,
     * Optional.ofNullable(replica), logClient)));
     * }
     */

    // Make shared state immutable

    // clientsGroup = Collections.unmodifiableList(clientsGroup);

    // MANUAL TEST

    // AbstractClient.ReadRequest rq = new AbstractClient.ReadRequest(0);
    // clientsGroup.get(0).tell(rq, ActorRef.noSender());
    // rq = new AbstractClient.ReadRequest(0);
    // clientsGroup.get(0).tell(rq, ActorRef.noSender());

    // AbstractClient.WriteRequest wq = new AbstractClient.WriteRequest(0, 5);
    // clientsGroup.get(0).tell(wq, ActorRef.noSender());
    // wq = new AbstractClient.WriteRequest(0, 8);
    // clientsGroup.get(0).tell(wq, ActorRef.noSender());

    /*
     * clientsGroup.get(0).tell(new AbstractClient.WriteRequest(0, 10),
     * Actor.noSender());
     * 
     * try {
     * Thread.sleep(6 * AbstractReplica.MAX_LATENCY + 200);
     * clientsGroup.get(0).tell(new AbstractClient.ReadRequest(0),
     * Actor.noSender());
     * } catch (InterruptedException e) {
     * 
     * }
     */

    // Write, crash, see if new leader is the correct one

    /*
     * WriteRequest wr = new WriteRequest(0, 7);
     * clientsGroup.get(0).tell(wr, Actor.noSender());
     * 
     * final Crash crashMsg = new Crash(Crash.Type.Now, 0);
     * final Map<Integer, ActorRef> rg = replicasGroup;
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(COORDINATOR_ID).tell(crashMsg, Actor.noSender());
     * }, 3000, TimeUnit.MILLISECONDS);
     */

    // Crash multiple coordinator one after

    /*
     * final Crash crashMsg = new Crash(Crash.Type.Now, 0);
     * final Map<Integer, ActorRef> rg = replicasGroup;
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(COORDINATOR_ID).tell(crashMsg, Actor.noSender());
     * }, 1000, TimeUnit.MILLISECONDS);
     * 
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(6).tell(crashMsg, Actor.noSender());
     * }, 7000, TimeUnit.MILLISECONDS);
     * 
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(5).tell(crashMsg, Actor.noSender());
     * }, 15000, TimeUnit.MILLISECONDS);
     */

    // Crash before synchronization: add the following code on
    // sendSynchronization message

    /*
     * final Crash crashMsgSync = new Crash(Crash.Type.ElectionSync, 0);
     * final Crash crashMsg = new Crash(Crash.Type.Now, 0);
     * final Map<Integer, ActorRef> rg = replicasGroup;
     * 
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(6).tell(crashMsgSync, Actor.noSender());
     * }, 1000, TimeUnit.MILLISECONDS);
     * 
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(COORDINATOR_ID).tell(crashMsg, Actor.noSender());
     * }, 1000, TimeUnit.MILLISECONDS);
     * 
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(5).tell(crashMsg, Actor.noSender());
     * }, 15000, TimeUnit.MILLISECONDS);
     * 
     * Executors.newSingleThreadScheduledExecutor().schedule(() -> {
     * rg.get(4).tell(crashMsg, Actor.noSender());
     * }, 25000, TimeUnit.MILLISECONDS);
     */

    /*
     * system.terminate();
     * 
     * System.out.println("\n========================================");
     * System.out.println("END");
     * System.out.println("========================================\n");
     */
  }
}
