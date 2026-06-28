package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Optional;

public class Client extends AbstractClient {
  Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica,
      Optional<ActorRef> listener) {
    super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
  }

  public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
    return Props.create(Client.class,
        () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
  }

  // Props method for automated tests
  public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay,
      Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
    return Props.create(Client.class,
        () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
  }

  @Override
  public void sendRead(ActorRef replica, int index) {
    log("READ request (" + index + ")");

    AbstractReplica.ReadRequest readRequest = new AbstractReplica.ReadRequest(index);
    replica.tell(readRequest, getSelf());
  }

  @Override
  public void sendWrite(ActorRef replica, int index, int value) {
    log("WRITE request (" + index + ", " + value + ")");

    AbstractReplica.WriteRequest writeRequest = new AbstractReplica.WriteRequest(index, value);
    replica.tell(writeRequest, getSelf());
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // Listener should be one client itself and should be invoked to log
        // read/write/timeout read/timeout write
        // TODO add your message handlers here .match(, )
        .match(ReadResult.class, this::callbackOnReadResult)
        .match(WriteResult.class, this::callbackOnWriteResult)
        .build();
  }
}
