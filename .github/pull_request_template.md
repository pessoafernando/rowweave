## What problem does this solve

<!-- The behaviour that was wrong or missing, and what it does now. -->

## Verification

<!-- Commands run, with the counts they reported. -->

- [ ] `mvn -B test -DexcludedGroups=bench,profile,multiprocess`
- [ ] `mvn -B test -Dgroups=multiprocess -DexcludedGroups= -Pcluster-isolated` (cluster-touching change)
- [ ] `bash tools/oss_docs_quality_gate.sh` (required for public API, configuration, behavior, monitoring, release/support, contributor, or documentation changes)
- [ ] `python3 tools/oss_docs_translation_gate.py` (translation change; the quality suite includes it)
- [ ] `CHANGELOG.md` updated for a released contract change

## Documentation maintenance

<!-- Required when a public contract, configuration value, runtime behavior,
monitor surface, release/support statement, runnable example, or translation
changes. The responsible author/reviewer is defined in documentation-maintenance.md. -->

- [ ] Every affected authoritative page is updated; planned behavior remains clearly distinct from supported behavior.
- [ ] Public API, Configuration, Runtime behavior, Monitoring, Release or support, Runnable example, and Translation evidence is recorded above.

## Invariants

<!-- Delete the lines that do not apply; keep the rest answered. -->

- Pure push: no `List<DataRow>` carrier introduced in `src/main`.
- Breaker working sets stay budgeted and spillable.
- Stream admission still driven by node and edge contracts, not by operator
  mix, operator count or fixed source/sink counts.
- Decimal semantics unchanged: `scale = 10`, `HALF_UP`, single rounding at
  output, and the null rules in `CONTRIBUTING.md`.
- A design note is attached if this changes an external contract, a persisted
  format, a recovery authority, the execution model, decimal semantics or the
  resource model.
- Support and security claims agree with the capability matrix; roadmap work is
  not described as available behavior.
