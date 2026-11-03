---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# ZooKeeper service registry

`rowweave-registry-zk` replaces the in-memory registry with a quorum-backed one:
Worker and Master identity, durable run state, checkpoint authority and leader
ownership all live in ZooKeeper, so a process that dies stops claiming to be
alive. This page states the session and lease model, the key space, the
authorities the registry carries, the secure-record rule and the failure
behaviour. The role model that decides who may hold a lease is in
[security](../operations/security.md); the topology the registry describes is in
[cluster architecture](../architecture/cluster.md).

## Choosing the registry

The engine core depends only on
[`ServiceRegistry`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/ServiceRegistry.java),
declared at `io/rowweave/cluster/registry/ServiceRegistry.java:14`, whose contract
at `:7-8` names the in-memory default and this plug-in. Nothing in core imports a
ZooKeeper type: the implementation is found through `ServiceLoader` against
[`ServiceRegistryProvider`](../../../rowweave-core/src/main/java/io/rowweave/cluster/registry/ServiceRegistryProvider.java),
whose `kind()` at `io/rowweave/cluster/registry/ServiceRegistryProvider.java:15` is
matched against the requested value, and whose
`ZkServiceRegistryProvider.kind()` at
`rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkServiceRegistryProvider.java:15`
answers `zk`.

| Property | Value that selects this module | Documented in |
|---|---|---|
| `rowweave.registry` | `zk` | [configuration](configuration.md) section 1.1 |
| `rowweave.zk.connectString` | `host:port,host:port,...` | [configuration](configuration.md) section 1.1 |

`ServiceRegistryBootstrap.create(...)` at
`io/rowweave/cluster/registry/ServiceRegistryBootstrap.java:23` treats a null or
empty kind, or `inprocess`, as the built-in registry at `:25`. Any other value
requires a matching provider on the classpath, and its absence fails as
`no ServiceRegistryProvider for kind=` with the missing module named
(`io/rowweave/cluster/registry/ServiceRegistryBootstrap.java:34-35`). A typo in
`rowweave.registry` therefore stops startup instead of quietly falling back to
memory, which is what a deployment with several Masters must not do.
`ZkServiceRegistryProvider.create(...)` at
`rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkServiceRegistryProvider.java:20`
applies the mirror rule: `zk` without a connect string is refused rather than
defaulted. Curator is a dependency of this module alone.

## Session and lease model

[`ZkServiceRegistry`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkServiceRegistry.java)
is declared at `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:53` and
opens its own ensemble client in the constructor at `:89`: a connect string that
does not work fails where it was written, not on the first submit. The client is
built with `ExponentialBackoffRetry(100, 3)`
(`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:115`), a five-second
shutdown budget for the underlying handle (`:70`, applied at `:116`), and session
and connection timeouts inherited from the Curator builder defaults (`:71-74`) —
this module deliberately does not invent its own defaults. Construction blocks
until the connection is established (`:121`) and creates the base path, tolerating
a concurrent creation (`:122-127`).

The lease *is* the ZooKeeper session. A registration is an ephemeral node, so it
disappears exactly when the session ends, and a process that was killed, lost
power or lost the quorum stops being visible to the cluster without any heartbeat
protocol to notice. That is why `renew(String)` at
`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:342` is an intentional
no-op, explained at `:343`: Curator keeps the session alive, and an explicit
renewal would only add a second, weaker liveness signal alongside the real one.
What this means operationally is that the session timeout, not a polling interval,
is the detection delay for a dead Worker.

`register(RegistryWorker)` at `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:286`
and `report(RegistryWorker)` at `:306` both go through one owner-checked write,
`upsertOwned(...)` at `:311`. That method reads the ephemeral owner before writing
and refuses a node owned by a different session
(`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:321-324`), so a stale
process cannot overwrite the record of a Worker that already re-registered with the
same id. Versioned writes are retried against four races — a competing create, a
node that vanished, a moved version, or a transport failure (`:328-336`) — and the
loop fails closed rather than spinning forever
(`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:338`).

`deregister(String)` at `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:347`
is owner-checked in the other direction too: a late close from an older incarnation
returns without deleting the new owner's node
(`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:355-357`). The delete is
versioned at `:359`, which is what produces the removal watch other processes see.

### Leader election

`tryClaimExclusive(RegistryWorker)` at
`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:291` is the election
primitive. It creates an ephemeral node at `:296` and returns `false` on a
`NodeExistsException` at `:298`, which is the whole algorithm: creation is atomic,
so exactly one Master wins the claim, and because the node is ephemeral, the winner
releases the role the moment its session ends. No lease timer, no fencing token
rotation and no reconciliation loop can disagree with that. The candidate that
loses re-reads the current leader rather than retrying the claim.

`liveWorkers()` at `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:373`
reads the children of the base path at `:376` and decodes each record; an absent
base path is an empty cluster, not an error (`:383-384`). `worker(String)` at `:392`
returns `null` for a missing id at `:396`. `watch(WatchListener)` at `:404`
registers a listener that receives the add, update and remove events produced by
the cache built over the base path at `:132`, and `WatchEvent` at
`io/rowweave/cluster/registry/ServiceRegistry.java:72` plus `WatchListener` at `:82`
are the delivery contract. A watch is an optimisation only: `liveWorkers()` remains
the authority, so a dropped event cannot make a dead Worker look alive.

`close()` at `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:409` closes the
cache, then the client, then every ZooKeeper handle the factory tracked. That last
step exists because Curator may replace its handle after a session loss, and an
abandoned handle from the replaced session would keep a reconnect thread alive
longer than the registry itself — the reason is stated at `:421-424` and the tracker
is declared at `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:426`.

## Authorities carried by the registry

The registry does not only discover processes; it is the durable authority for the
state a cluster cannot afford to lose when a Master changes. Each accessor returns
the same store family built over one client:

| Authority | Declaration | What it decides |
|---|---|---|
| Manifest commit | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:152` | which output commit won, and its lease |
| Run state store | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:157` | durable run and attempt state |
| Recoverable run catalog | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:220` | which runs can be recovered |
| Run lifecycle catalog | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:225` | lifecycle transitions |
| Run retention authority | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:230` | what may be purged |
| Run recovery ownership | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:241` | who may recover a run |
| Durable stage assignment | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:246` | assignment that survives a Master change |
| Stream checkpoint authority | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:200` | the pending and committed checkpoint decision |
| Stream checkpoint catalog | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:205` | retained checkpoint identities |
| Incremental frontier store | `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:162` | the durable frontier of an incremental feed |

The checkpoint and frontier paths are visible in the same constructor block, at
`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:138-141` for the state
family, and the resident CDC state described in
[resident CDC contract](stream-cdc.md) is durable because it is stored through these
authorities rather than held in a Master's memory. Each store is a class built over that
one client: `ZkRunStateStore`, `ZkManifestCommitAuthority`,
`ZkIncrementalFrontierStore` and `ZkUpgradeAuthority` in this module, and the
Stream checkpoint stores under the Stream runtime package. The recovery model they
implement is on
[recovery and state authority](../architecture/recovery.md).

Key-space roots are fixed per store and versioned by name, so an old layout is never
read as a new one: `/rowweave/workers` for membership
(`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:69`), and the run-state,
run-recovery, run-lifecycle, run-deadline, manifest-commit and
incremental-frontier roots are declared in their own store classes with the version
in the path.

### Store-upgrade admission

The single-argument constructor at
`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:89` never enables store
upgrade. The second constructor at `:95` takes an explicit upgrade authority root,
and when one is set, every state store is built against the active namespace that
the upgrade authority reports (`:133-137`). An invalid namespace path fails as
`active ZK namespace path is invalid` at
`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:214`. This is what makes a
store migration opt-in: a process cannot begin writing to a new layout because a
property was mistyped, and the procedure is in
[store migration](../../runbooks/store-migration.md).

## Records are secure or the registry refuses them

Membership records carry the deployment and authorization identity of the process
that wrote them, and the registry enforces that under the mTLS profile. The check is
made once, from the same property the control plane reads, at
`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:86-87`, and applied on write
at `:468` and on read at `:475`. A `MTLS` cluster therefore fails a registration
that carries no security identity rather than accepting an anonymous Worker, and a
record written before the profile was understood is not silently readable. The role
semantics behind that metadata are in
[security](../operations/security.md).

## Clients that find the cluster

[`ZkRemoteJobClient`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkRemoteJobClient.java)
at `io/rowweave/cluster/registry/zk/ZkRemoteJobClient.java:40` is a Batch `JobClient`
that needs only a connect string: it builds a registry (`:53`) and a
`RegistryMasterLocator`, then delegates to the ordinary real-cluster client (`:56`).
Because the locator resolves whoever is leader at the moment of each submit, a change
of Master stays invisible to the caller. The connection is opened in the constructor,
so a bad connect string fails there rather than at the first submit, and the class is
`AutoCloseable` so the session is released.

[`ZkStreamJobClient`](../../../rowweave-registry-zk/src/main/java/io/rowweave/cluster/registry/zk/ZkStreamJobClient.java)
at `io/rowweave/cluster/registry/zk/ZkStreamJobClient.java:20` is the Stream
equivalent, with a security configuration that is mandatory: the constructor at
`io/rowweave/cluster/registry/zk/ZkStreamJobClient.java:29` refuses a null or blank
connect string or a missing security configuration at `:31-34`, and owns an
independent session rather than sharing the Batch one. Both clients roll back a
partially constructed registry on failure (`io/rowweave/cluster/registry/zk/ZkRemoteJobClient.java:58`
and `io/rowweave/cluster/registry/zk/ZkStreamJobClient.java:41`).

## Failure and recovery behaviour

* Loss of quorum stops new claims and new registrations; already-running tasks are
  not killed by a registry outage, and the run resumes its authority reads when the
  quorum returns.
* Session expiry removes the ephemeral node, so a dead Worker leaves the membership
  set without an explicit deregistration, and a dead Master releases the leadership
  claim for whoever wins the next exclusive creation.
* A Worker that returns with the same id cannot take over a node still owned by its
  previous session (`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:321-324`),
  and a late close cannot delete the new owner's record (`:355-357`).
* A write that cannot converge after four rounds is an error, not a silent give-up
  (`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:338`, deregister at `:369`).
* Purging retained run state is authorised by the retention authority at
  `io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:230`, so a purge from a
  process that is not the current owner of the run is refused rather than executed.

## Qualification

`ZkServiceRegistryTest` — registration, listing, watch delivery, close.
`ZkWorkerLeaseFailoverTest` — session loss makes the Worker disappear and a restart
reclaims it. `ZkWorkerLeaseOwnershipTest` — the owner-checked write and the
late-close rule. `ZkManifestLeaderFailoverTest` — one winner per claim and the
leader change path. `ZkClusterProcessTest` — the registry driven from real separate
JVMs. `ZkRunPurgeTest` — retention authority and purge admission.
`ZkRunStateStoreTest`, `ZkRecoverableRunCatalogContractTest`,
`ZkRemoteJobClientDurableParityTest` — store contracts and parity with the
file-backed authorities. `ZkStreamCheckpointAuthorityTest` and
`ZkStreamCheckpointCatalogTest` — checkpoint decision durability.
`ZkStreamClusterRuntimeHaTest` — Master failover with live Stream state.
`ZkUpgradeAuthorityTest` — namespace and store migration admission.
`ZkMtlsStreamRealProcessTest` — secure records across real processes.
`ZkThreadReserveProfileTest` — the session-thread reserve the process capacity model
carries for this module (`io/rowweave/cluster/ProcessThreadCapacityModel.java:15`).

Related: [cluster topology and scheduling](../architecture/cluster.md),
[recovery and state authority](../architecture/recovery.md),
[deployment](../operations/deployment.md),
[security](../operations/security.md),
[configuration](configuration.md),
[run modes](../guides/run-modes.md),
[resident CDC contract](stream-cdc.md).
