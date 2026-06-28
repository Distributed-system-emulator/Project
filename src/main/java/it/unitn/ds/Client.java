package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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

    ReadTimeout readTimeout = new ReadTimeout(getSelf(), replica, index);
    getContext().system().scheduler().scheduleOnce(
        Duration.create(getReadTimeoutDelay(), TimeUnit.MILLISECONDS),
        getSelf(),
        readTimeout,
        getContext().system().dispatcher(),
        getSelf());
  }

  @Override
  public void sendWrite(ActorRef replica, int index, int value) {
    log("WRITE request (" + index + ", " + value + ")");

    AbstractReplica.WriteRequest writeRequest = new AbstractReplica.WriteRequest(index, value);
    replica.tell(writeRequest, getSelf());

    WriteTimeout writeTimeout = new WriteTimeout(getSelf(), replica, index, value);
    getContext().system().scheduler().scheduleOnce(
        Duration.create(getWriteTimeoutDelay(), TimeUnit.MILLISECONDS),
        getSelf(),
        writeTimeout,
        getContext().system().dispatcher(),
        getSelf());
  }

  public void onReadTimeout(ReadTimeout timeout) {
    // TODO: check if the relative request is already received

    callbackOnReadTimeout(timeout);
  }

  public void onWriteTimeout(WriteTimeout timeout) {
    // TODO: check if the relative request is already received

    callbackOnWriteTimeout(timeout);
  }

  @Override
  public final Receive createReceive() {
    return createBaseReceiveBuilder()
        // Listener should be one client itself and should be invoked to log
        // read/write/timeout read/timeout write
        // TODO add your message handlers here .match(, )
        .match(ReadResult.class, this::callbackOnReadResult)
        .match(WriteResult.class, this::callbackOnWriteResult)
        .match(ReadTimeout.class, this::onReadTimeout)
        .match(WriteTimeout.class, this::onWriteTimeout)
        .build();
  }
}
