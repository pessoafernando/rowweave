---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Security model

RowWeave has one security decision per boundary: the control plane admits a
peer by certificate identity, the data plane inherits that admitted identity,
and deployment artifacts are accepted only from signers named in configuration.
This page states those three rules with the properties, the certificate
requirements, the failure behaviour and the checks an operator can run. The
property tables themselves are in [configuration](../reference/configuration.md)
and are not repeated here.

## Stream security boundary

Stream isolates scheduling from execution and applies one admission and
lifecycle contract to every portable node.

## Process identity and roles

A Master accepts and schedules jobs; a Worker executes assigned tasks and
writes Outputs. Roles are configured explicitly. Do not expose a Client process
as a Master or Worker.

A role is not a configuration string that a process asserts about itself. It is
carried by the certificate the process presents, so a Client that points at a
Master port with a Worker certificate is admitted as a Worker and then refused
by the protocol it is not authorized to speak. [`ControlRole`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlRole.java)
closes the set at `io/rowweave/cluster/rpc/ControlRole.java:4-7`: `CLIENT`,
`MASTER`, `WORKER`.

The authorization file maps one certificate to one non-empty role set and is
parsed by [`ControlAuthzPolicy`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlAuthzPolicy.java),
declared at `io/rowweave/cluster/rpc/ControlAuthzPolicy.java:12`. Its line
format is `fingerprint=ROLE[,ROLE]`, enforced at
`io/rowweave/cluster/rpc/ControlAuthzPolicy.java:30`; a fingerprint is exactly
64 lowercase hex characters, checked at `:83`. Blank lines and `#` comments are
ignored. `roles(String)` at `io/rowweave/cluster/rpc/ControlAuthzPolicy.java:71`
returns an empty set for an unknown fingerprint, which is the only way a peer
becomes unauthorized. `authzSetId()` at `:77` is a digest over the canonical
form, which is what an operator compares between two runs to prove the policy
did not change.

## Profile selection and start-up ordering

The control profile is `rowweave.control.securityProfile`
([configuration](../reference/configuration.md) section 1.3), default `LOOPBACK`,
and the only other value is `MTLS`. The order of operations is the security
property: an unknown profile, or `MTLS` without the material below, is rejected
before any listener opens, so a process never accepts a connection under a
profile it failed to configure. `ControlSecurityConfig` reads the profile and
[`ControlTlsContext.load(Properties)`](../../../rowweave-core/src/main/java/io/rowweave/cluster/rpc/ControlTlsContext.java)
at `io/rowweave/cluster/rpc/ControlTlsContext.java:54` builds one immutable
snapshot per process.

All five properties below are required under `MTLS`; each is read through a
required-value helper at `io/rowweave/cluster/rpc/ControlTlsContext.java:55-59`,
and the two type properties default to `PKCS12` at `:60-61`.

| Property (see [configuration](../reference/configuration.md) section 1.4) | Read at | Requirement |
|---|---|---|
| `rowweave.control.tls.keyStore` | `io/rowweave/cluster/rpc/ControlTlsContext.java:55` | a readable regular file containing the local key and certificate |
| `rowweave.control.tls.keyStorePassword` | `:56` | non-empty |
| `rowweave.control.tls.trustStore` | `:57` | at least one valid certificate |
| `rowweave.control.tls.trustStorePassword` | `:58` | non-empty |
| `rowweave.control.tls.authzFile` | `:59` | a non-empty ASCII policy of at most 1 MiB |

`MAX_AUTHZ_BYTES` at `io/rowweave/cluster/rpc/ControlTlsContext.java:39` is the
policy ceiling, enforced at `io/rowweave/cluster/rpc/ControlTlsContext.java:265`
alongside the empty-file and non-regular-file rules at `:261`. A store that
cannot be read fails as `failed to load MTLS material: unreadable store` at
`:189`.

## Certificate requirements

Three independent checks run before the process is considered started.

**The local certificate must be in its own policy.** The loaded identity's
fingerprint is looked up in the authorization file, and an absent entry is fatal
with `local certificate fingerprint is absent from control authz policy` at
`io/rowweave/cluster/rpc/ControlTlsContext.java:70-71`. This is the rule that
makes rotation observable: a replacement certificate that was not added to the
policy stops the process instead of running with reduced rights.

**The key must match the certificate.** The private key is exercised against the
leaf public key at `io/rowweave/cluster/rpc/ControlTlsContext.java:236-243`, and
a mismatch reports `MTLS private key does not match leaf certificate public
key` at `:242`. A keystore assembled from unrelated material is therefore caught
at start-up rather than on the first handshake.

**The extended key usage must cover the configured roles.**
`requireExtendedKeyUsage` at
`io/rowweave/cluster/rpc/ControlTlsContext.java:272-281` reads the certificate's
EKU list. The client-authentication usage is always required; a process
configured as `MASTER` or `WORKER` must additionally carry the
server-authentication usage, because both roles listen. An absent EKU extension
is treated as unrestricted at `:275`, per X.509 and JSSE semantics, so a
self-signed certificate without EKU is accepted — a deployment that wants to
require the extension must issue certificates that carry it. A violation is
`local certificate EKU does not allow configured control roles` at `:280`.

The trust store is validated entry by entry: every X.509 alias is checked for
validity (`io/rowweave/cluster/rpc/ControlTlsContext.java:246-253`) and an empty
trust store is refused at `io/rowweave/cluster/rpc/ControlTlsContext.java:257`,
which stops a trust store that was created but never populated from silently
trusting nobody.

`fingerprint(X509Certificate)` at
`io/rowweave/cluster/rpc/ControlTlsContext.java:284-288` defines the identity
that the policy is written in: lowercase hex SHA-256 over the certificate's DER
encoding. That is the exact string an operator must put on the left of the `=`
in the authorization file.

## Peer admission

Every control RPC connection authenticates the peer before dispatching a frame,
through `authenticatedPeer(SSLSocket)` at
`io/rowweave/cluster/rpc/ControlTlsContext.java:126`. Three outcomes cover the
space, and a caller can tell them apart in the logs:

| Condition | Reported as | Site |
|---|---|---|
| no leaf certificate presented | `mTLS peer has no X509 leaf certificate` | `io/rowweave/cluster/rpc/ControlTlsContext.java:130` |
| valid certificate, no policy entry | `mTLS peer certificate is not authorized` | `io/rowweave/cluster/rpc/ControlTlsContext.java:136` |
| expiry, chain or handshake failure | `mTLS peer certificate validation failed` | `io/rowweave/cluster/rpc/ControlTlsContext.java:141` |

The leaf is also re-checked for validity at the moment of admission
(`io/rowweave/cluster/rpc/ControlTlsContext.java:133`), so a certificate stops
working at its not-after date without a restart. Roles for the admitted peer are
returned as a defensive copy, which is what `peerRoles` at
`io/rowweave/cluster/rpc/ControlTlsContext.java:122` exposes to the RPC layer.

`plaintext fallback` exists only for a local development profile. It must not
be enabled cross-host or in production. Production admission should require
MTLS and reject plaintext endpoints.

## The data plane does not re-decide

There is deliberately no independent shuffle security property. The data plane
follows the identity the control plane already admitted, and setting the
forbidden key is a start-up failure rather than a configuration choice — see
[configuration](../reference/configuration.md) section 1.5 and the transport
choice in [data-plane limits](../reference/data-plane-limits.md). This is why a
deployment cannot end up with an mTLS control plane and an open shuffle port:
the second decision does not exist to get wrong.

## Deployment payloads

Deployment artifacts are validated before distribution. Signed deployment
policy binds a Worker to an approved artifact identity. Do not accept unsigned
classes from job data or user functions.

The signer set is explicit and index-dense: `rowweave.deployment.signer.count`
with paired `rowweave.deployment.signer.id.<n>` and
`rowweave.deployment.signer.path.<n>` entries, and per-index manifest paths
`rowweave.deployment.manifest.<n>`
([configuration](../reference/configuration.md) section 1.8). A gap in the
index sequence, a duplicate signer identity, a relative signer path or a count
that disagrees with the entries fails deployment loading before any provider is
created, so an under-specified trust set cannot degrade to "trust the first
thing that parses". The ZooKeeper registry additionally refuses a membership
record without deployment and authorization identity when the profile is
`MTLS`, decided once from the same property the control plane reads
(`io/rowweave/cluster/registry/zk/ZkServiceRegistry.java:86-87`, applied at
`:468` and `:475`) — see [ZooKeeper registry](../reference/registry-zookeeper.md).

## Registry and HA

ZooKeeper membership is a security boundary as well as a discovery mechanism.
Restrict network access, credentials, and purge rights. A process becomes
eligible for Master election only when its configured registry identity and
role pass admission.

## Rotating material without widening access

A rotation is two file edits and a restart, and the checks above make a
mistake loud:

1. Issue the new certificate with the EKU required by its role.
2. Add the new fingerprint to the authorization file while the old entry is
   still present, so a half-rotated cluster keeps admitting both peers.
3. Restart one process and confirm it starts. A process that starts has passed
   the local-certificate, key-pair and EKU checks.
4. Compare `authzSetId()` before and after the change to confirm the policy
   moved deliberately. `ControlAuthzPolicyTest` qualifies the digest rule.
5. Remove the old entry only after every peer presents the new certificate.
   Removing it earlier makes the excluded peer fail admission as unauthorized
   rather than degrade.

`authz policy must not be empty`
(`io/rowweave/cluster/rpc/ControlAuthzPolicy.java:51`) is the backstop for a
rotation that truncated the file: no process starts with a policy that
authorizes nobody.

## Secure command template

The following is the shape of a cross-host start command under `MTLS`. Fill the
paths from the deployment's own secret handling; RowWeave reads a keystore
password as a property, so the command line below is only an example of the
shape and not a recommended place for the value.

Security does not change the resource contract. The process still declares its
complete envelope before roles are constructed, exactly as in
[deployment](deployment.md), so the `MTLS` properties are added to a command
that already carries the JVM limits.

```bash
ROWWEAVE_JVM=(\
  -Xms1g -Xmx2g -XX:MaxDirectMemorySize=1g \
  -XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m \
  -Xss1m -XX:ActiveProcessorCount=4 \
  -Drowweave.process.memoryLimitBytes=4294967296 \
  -Djava.io.tmpdir=/var/spool/rowweave)

java "${ROWWEAVE_JVM[@]}" -cp "$ROWWEAVE_CLASSPATH" \
  -Drowweave.control.securityProfile=MTLS \
  -Drowweave.control.host="$ADVERTISE_HOST" \
  -Drowweave.control.tls.keyStore="$KEYSTORE_PATH" \
  -Drowweave.control.tls.keyStorePassword="$KEYSTORE_PASSWORD" \
  -Drowweave.control.tls.trustStore="$TRUSTSTORE_PATH" \
  -Drowweave.control.tls.trustStorePassword="$TRUSTSTORE_PASSWORD" \
  -Drowweave.control.tls.authzFile="$AUTHZ_PATH" \
  -Drowweave.master.artifact.dir="$ARTIFACT_DIR" \
  io.rowweave.cluster.ClusterMain
```

A ZooKeeper-backed cluster adds `-Drowweave.registry=zk` and
`-Drowweave.zk.connectString`; see [deployment](deployment.md) for the full
start sequence and [ZooKeeper registry](../reference/registry-zookeeper.md) for
what the registry then enforces.

## Decimal and operational safety

Encrypted data-plane resource qualification covers the following exact
combinations with Netty `4.1.139.Final` and SunJSSE `17.0`:

| Platform | JDK vendor | Runtime version |
|---|---|---|
| macOS ARM64 | Homebrew | `17.0.15+0` |
| macOS ARM64 | Eclipse Adoptium | `17.0.20+8` |
| Linux ARM64 | Eclipse Adoptium | `17.0.20+8` |

CI execution jobs pin the Adoptium build and ARM64 runner architecture. Adjacent
patches, other architectures and other providers require separate resource
qualification; they are not admitted through a version wildcard.

Treat resource limits as security limits. Memory, thread, descriptor, and disk
admission prevent one job from exhausting a shared process. Security failures
fail the affected job and are recorded with typed categories rather than
leaving an unknown execution state.

## Tested by

`ControlAuthzPolicyTest` — the policy grammar, role and fingerprint rules, the
empty-policy refusal and the `authzSetId` digest. `ControlSecurityConfigTest` —
profile selection and the rejection of an unusable profile.
`RpcMtlsAuthorizationTest` — per-connection role admission from the policy.
`RpcSecurityFenceTest` — an unauthorized role cannot reach a fenced method.
`MtlsProcessClusterTest` — a real multi-process cluster under mTLS.
`StreamMtlsClusterIntegrationTest` — Stream admission over the same profiles.
`ZkMtlsStreamRealProcessTest` — secure membership records across processes.
`CertifiedTlsBuildMatrixTest` — the certified transport build matrix.
`DataPlaneTlsResourceProfileForkTest`, `TransportSecurityBootstrapForkTest` and
`WorkerServerMtlsConstructionTransactionForkTest` — start-up ordering, resource
bounds, and the absence of a window in which a listener is open before its
policy is loaded.

Related: [configuration](../reference/configuration.md),
[deployment](deployment.md),
[data-plane limits](../reference/data-plane-limits.md),
[ZooKeeper registry](../reference/registry-zookeeper.md),
[errors and refusals](../reference/errors-and-refusals.md),
[security policy](../../../SECURITY.md).
