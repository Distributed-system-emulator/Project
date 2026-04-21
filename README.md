# Distributed Systems Project 2026
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=java&logoColor=white)
![Akka](https://img.shields.io/badge/Akka-15A9CE?style=flat-square&logo=akka&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-Build-02303A?style=flat-square&logo=gradle&logoColor=white)

Repository for the **Distributed Systems** project assigned in the academic year **2025–2026**.  
The project is implemented in **Java** using **Akka Actors**.

## Setting up Gradle

To avoid version issues, you are required to use Gradle `9.2.1`.  
The easiest way to achieve this is by using a wrapper:

```bash
gradle wrapper --gradle-version 9.2.1
```

If you have an old version of Gradle, you can update it running

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install gradle 9.2.1
```

## Build, Run, and Tets the Project

```bash
./gradlew build
./gradlew run
./gradlew test
```

## Repository Structure

In the `main/java/it/unitn/ds` directory you will find the project base classes.  
`AbstractClient` and `AbstractReplica` contain the logic and structure that allows automated tests to work.  
Your implementation **must** use as base classes `Client` and `Replica` (which already inherit from their corresponding abstract classes).

### Logs

Please, **DO NOT print directly to standard output or file**. Instead, use the provided `Logger` class. Use the `log(String)` function to print "official" logs. Use `debug(String)` for debug prints. Both `AbstractClient` and `AbstractReplica` provide `log` and `debug` wrappers that prepend to your logs the string `[Client/Replica <ID>]` (resp.).

*Not adhering to this may cause automated tests to fail, even if your code works perfectly. This is due to prints slowing down you code. Tests are based on strict time intervals.*

## Client

As enforced by the abstract class, your `Client` class **must** implement the following methods:
- `public void sendRead(ActorRef replica, int index)`
- `public void sendWrite(ActorRef replica, int index, int value)`

During tests, these methods will be automatically invoked by the abstract class.  
In you test main function, if you want a client to send a message, do the following:
```java
int index = 0;
client.tell(new AbstractClient.ReadRequest(index), Actor.noSender());
```

For complete examples, please refer to `test/java/it/unitn/ds/base/APICompliance.java`.

To ensure automated tests can detect events in you system you **must** invoke the `AbstractClient.callback*(...)` methods whenever required, depending on the semantic of the callback.  
Follows a complete list of client-side callbacks. You can find more info in the related Java docs in the code.

- `callbackOnReadResult(AbstractClient.ReadResult)`
- `callbackOnWriteResult(AbstractClient.WriteResult)`
- `callbackOnReadTimeout(AbstractClient.ReadTimeout)`
- `callbackOnWriteTimeout(AbstractClient.WriteTimeout)`

## Replica

Analogously to `Client` your `Replica` implementation **must** implement some methods and invoke callbacks.

*Methods*

- `public int getSystemNumberOfActors()`
- `public void crash(AbstractReplica.Crash how)`
- `public void initSystem(AbstractReplica.InitSystem sysInit)`

*Callbacks*

- `callbackOnUpdateApplied(int index, int value)`
- `callbackOnElectionStarted(int crashedCoordId)`
- `callbackOnCoordinatorElected(int newCoordID)`

As for the client, you can find more documentation in the Java docs.

The size of positions list **must** be equal to `AbstractReplica.POSITIONS_LIST_LENGTH`.

Coordinators **must** send heartbeats every `AbstractReplica.getCoordinatorBeatInterval()` milliseconds.

### Emulated Network Delays

Automated tests expect to see particular messages after calculated time intervals. These intervals are computed based on `AbstractReplica.getMinLatency()` and `AbstractReplica.getMaxLatency()` (these latencies will be set by the `AbstractReplica` class constructor). It is therefore fundamental that your replica implementation conforms to these delays. Delays are always expressed in MILLISECONDS.

`AbstractReplica` provides a `void tell(Serializable m, ActorRef dst)` method which emulates network latency ensuring messages are delivered in FIFO order (`NetworkChannel` class). **Not using this method may cause some tests to fail (even if the implementation is correct).** 

*Hint: in your replica implementation, create the `tell`, `broadcast`, and `multicast` functions to handle messages. This way, you will unify the handling of crashes.*

## How do tests work?

In the `test/java/it/unitn/ds/base` directory you can find some basic tests that allow to verify basic system functionalities. First of all, refer to `APICompliance.java`.

In general, each test works as follows:
1) Initialize a new system using the `AbstractReplica.InitSystem` message.
2) Whenever an actor (`Client` or `Replica`) is instantiated, the test passes to it a `probe` (or `listener`), which is a special kind of actor (`akka.testkit.javadsl.TestKit`). This is achieved via the `Client/Replica.propsWithListener(...)` method.
3) The test will send read/write requests and emulate crashes using the abstract methods and pre-defined message classes of clients and replicas.
4) The `callback*(...)` functions will forward messages to the `probe` which will then be able to verify that the system behaves as intended.

In summary:
1) Abstract classes ensure test have a standardized way to send read and write requests from clients and emulate crashes.
2) Properly invoked callbacks ensure that probes receive the required information to verify the system's behavior.

Thus, **it is crucial that you follow `Client` and `Replica` guidelines** (described above).

**Before you submit you project, all base tests must pass.**  
*Submissions that do not meet this requirement will be ignored.*

## Note

This is the first time we automate the tests. If you find issues in the base classes or base tests, let us know via email or telegram group.

## Contributors

- Stefano Genetti [stefano.genetti@unitn.it]
- Thomas Pasquali [thomas.pasquali@unitn.it]
