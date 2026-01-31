
# S2C — Shared Storage Consensus

<img src="assets/art.jpg" align="right" width="350" alt="artwork" />

S2C is a strongly consistent, cloud-native, quorum-less, state machine replication (SMR) system built atop AWS S3 that can remain **available and strongly consistent with even a single live node**.

Effectively, **S2C transforms S3 into a significantly more capable system with richer semantics that you can tailor to your needs.**

Instead of peer-to-peer quorum, S2C uses S3 (or any S3-compatible storage with similar guarantees) to *achieve consensus* for the replicated state machine, leveraging its strong consistency model and conditional writes. S3 is used for:

- leadership
- log commits
- linearizable reads
- snapshots
- exactly-once command semantics[^1]

To overcome S3's latency and costs, S2C batches commands and reads and does not perform any polling.


### Internals

S2C has a comprehensive deep dive document that explains the internals and architecture. You can find it in the [S2C Deep Dive](docs/deep-dive.md).

### Why S2C?

Many of us have tried to use S3 for structured data or state management in a distributed application to avoid the high operational cost and complexity of running a quorum-based consensus system, but this often results in significant complexity and fragility.

S2C changes that by building a **real** replicated state machine directly on top of S3, providing the same correctness guarantees as quorum-based consensus systems while inheriting S3's high availability and durability, all with a **fraction of the operational complexity**.

All you need to do to embed a replicated state machine into your application is provide an S3 client and the state machine implementation. No quorum. No disks to babysit.

## Core properties and guarantees

- Strong consistency
- Linearizable reads and writes
- Built-in exactly-once command semantics[^1]
- Nodes join dynamically with elastic cluater size.
- Survives full cluster shutdown
- Single-node availability
- High durability
- Split-brain safe by construction
- Cold-startable with full state recovery from zero nodes
- Not subject to clock skew as no clocks or leases are used for consensus
- Multi cluster (groups) support for indefinite sharding.

### High-level design

At its core, S2C has a simple design:

- There is a single authoritative LeaderState stored in S3
- Log, leadership, commits, and reads are fenced using S3 ETags
- Only one node can successfully act as leader at a time
- Any node can join the cluster dynamically without affecting leader availability
- Followers have their state machines real-time synced via RPC and can take over leadership in case of leader failure (optional)

So compared to quorum systems:

- S3 replaces quorum
- ETags replace voting
- Conditional writes replace leader leases

### When to use S2C

- You need a plug-and-play strongly consistent, replicated state machine that you can just embed in your application, without needing to manage a quorum or to run a separate consensus cluster.
- You are already using S3 or S3-compatible storage in your architecture.
- You can tolerate S3 latency (albeit mitigated by batching).
- You want to simplify your consensus implementation by offloading it to S3.
- You need exactly-once command semantics and linearizable reads and writes[^1].
- You need to cold-start from zero nodes and survive full cluster shutdowns.

## Typical use-cases

S2C is a great fit for:
- Embedded replicated state machines for your distributed application
- Control planes
- Metadata services
- Configuration management
- Distributed locks / coordination
- Durable counters, registries
- It can even be used to build a fully-fledged latency-insensitive database.

### What S2C is not

- S2C is not a drop-in replacement for Raft or Paxos for all scenarios. 
- S2C is not for ultra-low latency use-cases where every microsecond counts (limited by S3 latency).
- It is not usable when you need to run a consensus system without having an S3-compatible storage.

### Quick Start

Prerequisites: Java 21+

**Gradle**

S2C is not yet available on Maven Central, but you can use JitPack to include it in your project:

```groovy
repositories {
    mavenCentral()
    maven { url 'https://jitpack.io' }
}

dependencies {
    implementation 'com.github.io-s2c:s2c:0.1.0-alpha'
}
```

First, define your state machine:

```java
public class ReplicatedCounter extends S2CStateMachine {
    private long counter = 0;

    @Override
    public ByteString snapshot() {
        return ByteString.copyFromUtf8(String.valueOf(counter));
    }

    @Override
    protected void loadSnapshot(ByteString snapshot) {
        String s = snapshot.toStringUtf8();
        counter = s.isEmpty() ? 0 : Long.parseLong(s);
    }
    
    @Override
    protected ByteString handleRequest(ByteString request, StateRequestType type) throws ApplicationException {
        String cmd = request.toStringUtf8();
        if (cmd.equals("INC")) {
            counter++;
            return ByteString.EMPTY;
        } else if (cmd.equals("GET")) {
            return ByteString.copyFromUtf8(String.valueOf(counter));
        }
      	throw new ApplicationException("Unknown command %s".formatted(cmd));
    }

    // Command (Strongly Consistent Write)
    public void increment() {
        sendToLeader(ByteString.copyFromUtf8("INC"), StateRequestType.COMMAND);
    }

    // Read (Linearizable)
    public long get() {
        StateRequestResponse res = sendToLeader(ByteString.copyFromUtf8("GET"), StateRequestType.READ);
        return Long.parseLong(res.getApplicationResult().getBody().toStringUtf8());
    }
}
```

Then, initialize and run S2C:

S2C has a very easy to use API:

```java
// 1. Create S3 client
S3Client s3Client = S3Client.builder()
    .region(Region.US_EAST_1)
    .build();

S3Facade s3Facade = S3Facade.createNew(s3Client);

// 2. Define Node Identity
NodeIdentity nodeIdentity = NodeIdentity.newBuilder()
    .setAddress("10.0.0.1")
    .setPort(8080)
    .build();

// 3. Configure S2C
S2COptions s2cOptions = new S2COptions()
    .snapshottingThreshold(100)
    .requestTimeoutMs(10_000);

// 4. Create Server and Node
S2CServer s2cServer = new S2CServer(nodeIdentity, s2cOptions);

S2CNode s2cNode = S2CNode.builder()
    .bucket("my-s2c-bucket")
    .nodeIdentity(nodeIdentity)
    .s2cGroupId("my-cluster") // For multi-cluster, many S2CNode instances can happily coexist in the same process, just use different group IDs!
    .s2cOptions(s2cOptions)
    .s2cServer(s2cServer)
    .s3Facade(s3Facade)
    .build();

// 5. Register your State Machine
ReplicatedCounter counter = s2cNode
    .createAndRegisterStateMachine("counter", ReplicatedCounter::new);

// 6. Start
s2cServer.start();
s2cNode.start();

// 7. Use your State Machine
try {
	counter.increment(); // Execution delegated to leader
	counter.get() // Linearizable read
} catch (ApplicationException e) {
	// Your state machine rejected the request
}
```

### Status

S2C is currently in alpha and should be considered experimental as it has not yet been deployed in production environments.

However, it has passed extensive chaos and fault-injection tests and has proven recovery from crashes, partitions, and leader failure. Also, formal verification is planned.

Because of this, usage and feedback are highly encouraged and appreciated.

### Contributing

Contributions are welcome! Please open an issue or submit a pull request.

Contributions in the form of:

- More tests (Jepsen-style) and benchmarks
- Implementations in other languages (e.g. Go, Rust, C++, Python)
- Support for other storage backends that preserve the same semantics

Are highly appreciated.

The [deep dive document](docs/deep-dive.md) should be treated as the normative specification; alternative implementations are expected to preserve the documented semantics.

Also, bug reports and feature requests are welcome.

[^1]: S2C can only guarantee exactly-once command semantics for nodes that have unique and stable node identities (i.e. IP address and port).
