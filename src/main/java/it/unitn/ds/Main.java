package it.unitn.ds;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {
  public static void main(String[] args) {
    System.out.println("========================================");
    System.out.println("START");
    System.out.println("========================================\n");

    final int N_REPLICAS = 4;
    final int N_CLIENTS = 4;
    final int COORDINATOR_ID = 0;
    final ActorSystem system = ActorSystem.create("TestMain");

    Logger.setDestinationStdout();
    Logger.setDebugEnabled(true);

    Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
    for (int i = 0; i < N_REPLICAS; i++) {
      replicas.put(i,
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
     * replicas.put(i,
     * system.actorOf(
     * Replica.propsWithListener(i, AbstractReplica.MIN_LATENCY,
     * AbstractReplica.MAX_LATENCY,
     * AbstractReplica.COORDINATOR_BEAT_INTERVAL, logReplica),
     * "Replica_" + i));
     * }
     */

    replicas = Collections.unmodifiableMap(replicas);

    InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
    for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
      entry.getValue().tell(initMsg, ActorRef.noSender());
    }

    // TODO: Create your clients
    List<ActorRef> clientsGroup = new ArrayList<ActorRef>();
    for (int i = 0; i < N_CLIENTS; i += 1) {
      ActorRef replica = replicas.get(i % replicas.size());
      clientsGroup.add(system.actorOf(Client.props(200, 300, Optional.ofNullable(replica))));
    }

    /*
     * //Alternative client with a client logger
     * final ActorRef logClient = system.actorOf(Client.props(200, 300,
     * Optional.ofNullable(replicas.get(0))));
     * for (int i = 0; i < N_CLIENTS; i += 1) {
     * ActorRef replica = replicas.get(i % replicas.size());
     * clientsGroup.add(system.actorOf(Client.propsWithListener(200, 300,
     * Optional.ofNullable(replica), logClient)));
     * }
     */

    // Make shared state immutable
    clientsGroup = Collections.unmodifiableList(clientsGroup);

    // TODO: Implement your main logic

    system.terminate();

    System.out.println("\n========================================");
    System.out.println("END");
    System.out.println("========================================\n");
  }
}
