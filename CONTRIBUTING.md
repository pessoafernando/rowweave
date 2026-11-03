---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Contributing to RowWeave

RowWeave is an embedded batch and bounded-stream compute engine. Contributions
are reviewed against the engine invariants below.

## Absolute invariants

1. Pure push pipeline: rows flow from source through operators to output
   without materialising an intermediate row list.
2. No `List<DataRow>` as a data carrier in production code (`src/main`). The only
   permitted materialisation is the internal working set of a breaker operator
   (sort buffer / aggregation buffer), and it must be budgeted and spillable.
3. Operator signatures: pass-through `void apply(RowStream in, RowSink out)`,
   unary breaker `void apply(RowStream upstream, RowSink out)`, binary
   `void apply(RowStream left, RowStream right, RowSink out)`. Entry is always
   `RowStream`, exit is always `RowSink`.
4. One job compiles to one plan graph and is submitted whole; the output is a
   sink node executed by the worker, not a consumer drained by the client.
5. Stream admission is driven by node and edge contracts plus graph-wide
   invariants. Selecting an executor by operator mix, operator count, or fixed
   source/sink counts is not accepted.

## Decimal semantics

The user-facing contract is
[decimal and null semantics](docs/product/reference/decimal-null-semantics.md).
The table below is the same rules in contributor shorthand.

| Case | Rule |
|---|---|
| BigDecimal scale | 10 decimal places, `RoundingMode.HALF_UP` |
| BigDecimal rounding | full-precision accumulation, single `setScale(10, HALF_UP)` before output |
| null in grouping | groups on its own, never dropped |
| null in numeric sort | equal to 0 by default, configurable NULLS FIRST/LAST |
| null in string sort | smallest by default, configurable |
| null in join | null never matches null |

## Development workflow

Build requires JDK 17; released bytecode targets Java 8. Start with the narrow
check for what you changed and widen only after it is green:

Production compilation uses `--release 8` to check Java API availability as well
as class-file versions. Tests run on the JDK 17 build host. Packaged consumer
qualification separately compiles and runs against a real JDK 8.

```bash
mvn -q compile           # compile only
mvn -q test              # all modules, default groups
mvn -q -pl rowweave-core test
```

The default suite excludes the `bench`, `profile` and `multiprocess` tags, so a
`-Dtest` selection never reaches them. Run the multi-process cluster suite the
way continuous integration does, with one JVM per test class so that sockets,
threads and ports are released between classes:

```bash
mvn -q test -Dgroups=multiprocess -DexcludedGroups= -Pcluster-isolated
```

Documentation changes run the whole documentation suite and rebuild the local
site:

```bash
bash tools/oss_docs_quality_gate.sh
python3 tools/oss_docs_site.py --out target/docs-site
```

### What continuous integration runs

`.github/workflows/ci.yml` has five jobs. A pull request is not green because
the default suite passed; it is green when the jobs that cover the change
passed, and each job is a separate failure category to read.

| Job | Coverage | Command |
|---|---|---|
| `surface` | public-surface guards, run before Maven because they are textual and fast | `bash tools/oss_identity_residue_gate.sh`, `bash tools/oss_content_boundary_gate.sh`, `python3 tools/oss_java_identifier_gate.py`, `python3 tools/oss_source_language_gate.py`, plus a repository-wide check that every relative Markdown link resolves |
| `documentation` | documentation is a product surface, so it is checked on every change rather than only when the diff visibly touches Markdown | `bash tools/oss_docs_quality_gate.sh` |
| `unit` | operators, unit and architecture tests on Linux and macOS | `mvn -B -ntp test -DexcludedGroups=bench,profile,multiprocess` |
| `cluster` | real socket data plane, failover and timeout injection on both platforms | `mvn -B -ntp test -Dgroups=multiprocess -DexcludedGroups= -Pcluster-isolated` |
| `release` | packaging and a real Java 8 consumer, from a clean checkout | `bash tools/oss_release_gate.sh`, `mvn -Prelease -B -ntp verify -Drowweave.java8.home=... -Drowweave.java8.required=true`, `bash tools/oss_example_qualification.sh` |
| `security` | every resolved dependency scope checked against OSV | `mvn -B -ntp dependency:tree > dependency-tree.log`, `python3 tools/oss_dependency_security_gate.py dependency-tree.log` |

Every checkout fetches complete history. The identity gate rejects shallow
repositories; pull requests scan their actual head commit, excluding generated
merge metadata. Gate regression tests exercise a deleted historical identity
and a shallow clone, as well as incomplete security inputs.

Two of these jobs are the reason a purely local run can pass while a pull
request fails. `surface` rejects a non-English comment, a retired identifier, or
a dead link in any tracked file, including Markdown. `unit` and `cluster` run on
macOS as well as Linux because `close()` behaviour, `InputStream.available()`
and half-close through `shutdownOutput()` differ at the operating-system
boundary; a socket-lifecycle regression is caught by that matrix, not by the
transport implementation. The `release` job turns "the artifact set is defined"
into "it was built, checksummed and consumed": it exports a JDK 8 home, requires
the release-profile SBOM and checksums, and treats a skipped Java 8 consumer
integration test as a failure rather than a pass, because
`-Drowweave.java8.required=true` makes an unforwarded property or an incomplete
JDK 8 visible instead of silent.

Transport execution jobs use ARM64 runners (`ubuntu-24.04-arm` and `macos-15`)
and Temurin `17.0.20+8`, matching the exact TLS qualification matrix. A JDK or
Netty update requires new transport resource qualification before changing the
CI runtime pins.

Run the job that owns your change locally before pushing. The gate scripts are
the same commands the job runs, so a local run reproduces a CI failure without a
push cycle.

Changes follow RED, minimal GREEN, refactor on one behaviour slice at a time.
Keep the slice self-describing: cover its boundary cases, error branches and
failure interleavings in the same RED set rather than spreading them over
several commits.

Design decisions that change an external contract, a persisted format, a
recovery authority, the execution model, decimal semantics, or the resource
model are recorded as a reviewed design note attached to the pull request, and
released contract changes are summarised in `CHANGELOG.md`. Implementation slices
and bug fixes do not
need their own record.

## Issue intake and support boundary

Use the bug template for reproducible wrong results, recovery failures,
resource failures, startup failures, and documentation defects. Use the feature
request template for unsupported outcomes, stable refusals, and lifecycle
requests. Current-version scope, reproduction data, and escalation boundaries
are in [SUPPORT.md](SUPPORT.md). Security issues never enter public issue
triage; follow [SECURITY.md](SECURITY.md).

The capability matrix is the support authority. Roadmap items are not support
claims, and a merged contributor patch does not by itself widen certified
behavior. If an issue has no self-contained reproduction, triage asks for one
and may close it while the request is inactive.

## Review boundaries

Reviews check behavior coverage, public-contract compatibility, capability and
support claims, English documentation, identity gates, and release evidence.
A design note is required only for the external-contract changes listed above.
Contributors should keep unrelated formatting, refactoring, and capability work
out of the same change. Artifact publication is a separate owner-authorized
action and is never implied by merging or passing local gates.

## Repository layout

| Module | Purpose | Application dependency? |
|---|---|---|
| `rowweave-core` | Engine, public Batch/Stream API, DAG admission, runtimes, recovery, and release contracts. | Yes, normally the only required engine dependency. |
| `rowweave-pipeline` | Versioned dataset hand-off, codec, and durable Pipeline boundary. | Only for Pipeline integrations. |
| `rowweave-connector-s3` | Tested exact-snapshot object-storage source. | Only for that connector. |
| `rowweave-connector-postgres-cdc` | Bounded PostgreSQL CDC source and frontier contract. | Only for that connector. |
| `rowweave-registry-zk` | Optional ZooKeeper election and discovery for HA clusters. | Only for ZooKeeper mode. |
| `rowweave-monitor` | Optional read-only HTTP dashboard, JSON API, and Prometheus endpoint. | Only when monitoring is enabled. |
| `rowweave-soak` | Release qualification and soak harnesses. | No. |
| `rowweave-examples` | Runnable API examples and example tests. | No. |

Product documentation lives under `docs/product`; operator runbooks under
`docs/runbooks`; automated editorial and release gates under `tools`.

## Test strategy

Start with the narrow verification for the changed module and broaden only after
it is green:

```bash
mvn -q -pl rowweave-core test
mvn -q test
```

Use one coherent behavior slice per change. Cover its boundaries, typed error
branches, and failure interleavings in the same RED set, then implement minimal
GREEN and refactor without changing behavior. Cluster-isolated multiprocess
tests use the explicit `cluster-isolated` profile in `rowweave-core`.

Documentation changes run `bash tools/oss_docs_quality_gate.sh`. Build the
local site with `python3 tools/oss_docs_site.py --out target/docs-site`, then
open `target/docs-site/index.html`. The command converts the curated
Markdown inventory, rewrites page links, generates navigation and search, and
links source citations to their repository locations. Product-facing
changes that alter public text or examples also run the relevant behavior and
contract tests. Release candidates use the stricter path in
[Release candidate packaging](#release-candidate-packaging).

## Documentation requirements

Documentation maintenance triggers, page ownership, reviewer evidence, and
continuous-integration policy are defined in
[documentation maintenance](docs/product/reference/documentation-maintenance.md).

English is the authoritative language for repository documentation. Keep
canonical Markdown under its product, runbook or repository-level path.

The same standard applies to code: comments, javadoc, resource text, test
literals and repository scripts are English, across every tracked file rather
than only the shipped source. `python3 tools/oss_source_language_gate.py`
reports every CJK line in the tracked tree, names the files it scanned, and
proves its own detector by rejecting a Chinese comment planted in a temporary
file before it reports a clean tree.

Translations are permitted only under `docs/i18n/<locale>/`, using the same
path as their English source page. Follow
[translation synchronization](docs/product/reference/translation-synchronization.md):
register the page in the locale manifest, bind it to the final English content
digest, preserve normative and supported-version statements, and obtain a
source-language review. Do not use a translation to introduce or override
behaviour, configuration, limits, security guidance, release status or
compatibility claims. Update the English source and all listed translations in
the same coherent change, then run `python3
tools/oss_docs_translation_gate.py`.

## Test identities and credentials

Certificates, truststores and passwords created by the test and soak harnesses
are ephemeral fixtures. Each harness generates its own keypair at run time,
keeps the private key in a temporary directory or in memory, and never writes a
signing key into the repository. The literal `changeit` used as a keystore
password appears only in test and soak code paths that create those throwaway
identities.

Production deployments supply their own key material out of band. The engine
holds no built-in credential, no default password and no vendor endpoint, and a
cluster in `MTLS` mode refuses to start without an explicitly configured
keyStore, trustStore and authorization policy. When you add a test that needs
an identity, generate it in the same way rather than checking a key file in.

## Release candidate packaging

A release candidate is a clean commit, not a dirty worktree. Build with JDK 17,
and set `ROWWEAVE_JAVA8_HOME` to a real Java 8 home for consumer verification:

```bash
export ROWWEAVE_JAVA8_HOME=/path/to/jdk8
mvn -q test
mvn -Prelease verify -Drowweave.java8.home="$ROWWEAVE_JAVA8_HOME" \
    -Drowweave.java8.required=true
bash tools/oss_release_gate.sh
bash tools/oss_docs_quality_gate.sh
```

The release profile and gate produce, for each of the eight published modules:

* `rowweave-core-<version>.jar`, `-sources.jar`, and `-javadoc.jar`;
* `bom.json` and `bom.xml` CycloneDX SBOMs;
* a `.SHA-256` sidecar for every binary JAR and SBOM.

The gate verifies all sidecars and refuses to silently skip the Java 8
consumer matrix. `tools/java8-home.sh` is the single resolution contract behind
every Java 8 claim in the repository, and a consumer IT named as required fails
instead of skipping when no JDK 8 resolves. The compatibility release packet is separately reproducible
from `release-manifest.hex`, its detached `release-manifest.sig.hex`, and
`release-public-key.der.hex`; its signer key is generated offline and is not
persisted in the repository. Do not conflate that signed contract packet with
artifact publication.

## Publication prerequisites

Publication is a separate, explicit authorization step. This repository has no
workflow that pushes a release, and artifacts are never published automatically
from `mvn package` or the local gate.

A failed or incomplete gate means do not publish the candidate.

Before publication, record the clean commit, release version, full test and
release-gate evidence, artifact and SBOM checksums, signed contract-packet
verification, documentation gates, and the license/third-party review result.
Publish through the authorized Central Portal workflow, apply the repository
owner's required artifact-signing procedure, and verify signatures, checksums,
and SBOMs from the downloaded artifacts before announcing the release. Do not
publish on a dirty tree, from an unqualified Java 8 matrix, or with missing
checksum evidence.

### Review checklist

Each item below is a state the repository already holds, a command that
produces the evidence, or an owner decision that has to be recorded before the
first push. A reviewer signs off on this list, not on a re-derivation of it.

Repository state:

* The candidate is a clean commit: `git status --porcelain` is empty and
  `git diff --check` is clean.
* Every author and committer in reachable history is the declared publishing
  identity. The zero-residue gate asserts this over all of history, so the
  check is not done by eye on the last few commits.
* No tag, no GitHub release, and no git remote exist until the owner creates
  them.
* Commit signing is an owner decision. The candidate history is unsigned today;
  the review records whether the published branch must be signed.

Identity and public vocabulary:

* `bash tools/oss_identity_residue_gate.sh` passes. It sweeps the working tree,
  reachable commit metadata, and every object reachable from any reference, so
  an identifier that one commit carried and a later commit renamed away cannot
  survive in the published history.
* `python3 tools/oss_java_identifier_gate.py` passes. No published type or
  method name carries a predecessor stem.
* `bash tools/oss_content_boundary_gate.sh` passes. Published content describes
  behavior and contribution rules, not private planning history or internal
  milestone identifiers.
* `python3 tools/oss_source_language_gate.py` passes. Comments, javadoc,
  resource text, test literals and repository scripts are English in every
  tracked file, so a first-time contributor can read the engine without
  Chinese. The gate rejects Han ideographs and CJK punctuation only, which
  leaves non-CJK characters that carry meaning alone: an em dash, a Greek pair
  of grouping keys, or an emoji in a round-trip fixture. A fixture that exists
  to prove multi-byte width keeps the exact same character written as an escape
  sequence, because the runtime string is then byte-identical while the source
  carries no CJK code point.

Coordinates and namespace:

* The parent `pom.xml` groupId is `io.github.rowweave`, and the repository URL
  is a single property that every page, template, and citation derives from.
  Set that property to the account that will actually own the repository before
  the first push, because the documentation site cites source files through it.
* The namespace under that groupId must be claimed or verified by the publishing
  account. A publishing account that does not control the declared namespace is
  a blocker, not something fixed after the release.
* The developer identity in the parent `pom.xml` is a project-level identity,
  not a personal one.
* `distributionManagement` declares only the snapshot repository for the
  Central Portal. There is no release deployment repository and no signing or
  staging plugin configured, so artifact release is deliberately an external,
  authorized workflow rather than a build step.

Governance files and license:

* The governed set is present: `LICENSE`, `NOTICE`, `README.md`,
  `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`, `SUPPORT.md`, `ROADMAP.md`,
  and `THIRD-PARTY-NOTICES.md`.
* A code-of-conduct file is not shipped with `0.2.0`. Adding one is an owner
  decision that changes the governed set and the issue-intake wording.
* The license-header decision is still open and must be recorded, not inferred:
  no file under `src/main/java` carries a per-file SPDX header line, while
  `LICENSE` holds the full license text and `NOTICE` names the copyright
  holder. Either accept that shape for publication or normalize the headers in a
  separate mechanical pass.
* `THIRD-PARTY-NOTICES.md` is current. Regenerate it with `mvn -o -B
  dependency:tree`, flatten the `[INFO]` output lines to bare coordinates, then
  run `python3 tools/oss_third_party_notices.py`. The only expected difference
  from the committed page is the generated front matter and date; the
  redistributed, runtime, and build-only categories must add up to the same
  coordinate count.

Documentation:

* `bash tools/oss_docs_quality_gate.sh` passes as a suite, including the anchor
  check, the language policy, the translation parity check, the maintenance
  check, and every architecture and configuration parity check.
* `python3 tools/oss_docs_site.py --out target/docs-site` builds the site.
  `target/` is ignored, so no generated page enters the candidate.
* Source citations in the published site resolve only once the repository is
  public. A 404 on those citations before the first push is expected and is not
  a documentation defect.

Artifacts:

* `ROWWEAVE_JAVA8_HOME=/path/to/jdk8 bash tools/oss_release_gate.sh` passes on
  the candidate commit and produces, for each published module, the binary
  image, sources image, javadoc image, SBOM, and checksum sidecars. The gate
  refuses to run when the Java 8 home is missing or is not a real JDK 8, so a
  skipped consumer matrix cannot be reported as a pass.
* The compatibility `release packet` is separately reproducible from
  `release-manifest.hex`, `release-manifest.sig.hex`, and
  `release-public-key.der.hex`, and its verification is recorded in the review.
  That signed packet is a contract artifact and is not artifact publication.
* Downloaded artifacts are re-verified against the recorded checksums and SBOMs
  from outside the build machine before any announcement.

Hosted repository settings:

* The owner sets the repository name, description, homepage, topics, and default
  branch, then requires the continuous-integration jobs defined in
  `.github/workflows/ci.yml` as branch-protection checks: `surface`,
  `documentation`, `unit`, `cluster`, and `release`.
* Issue intake is already wired: blank issues are disabled and the bug and
  feature templates ask for the environment, reproduction, and capability
  boundary fields named in [SUPPORT.md](SUPPORT.md).
* Private vulnerability reporting must be enabled so that reports arrive through
  the route stated in [SECURITY.md](SECURITY.md) rather than through public
  issues.

Known open items, recorded so that silence is not read as approval:

* The [documentation audit](docs/product/reference/documentation-audit.md)
  keeps the remaining non-green rows open: the per-method API and function
  references that are still summaries, the connector task and packaging
  walkthroughs, and the comments inside test sources. None of them changes
  supported behavior, and the open rows are listed rather than implied.
* Long-running stability evidence is deferred to the final release acceptance.
  No page presents it as completed.

Passing this checklist qualifies a candidate. It does not authorize a push, a
tag, a release, or an announcement, and none of those happen until the owner
acts.

## Pull requests

Describe the problem the change solves, the resulting behaviour, and how it was
verified. Keep the diff scoped to one coherent change.
