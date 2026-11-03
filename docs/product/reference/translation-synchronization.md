---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Translation synchronization

English is the source of truth for RowWeave documentation. A translation is a
reviewed mirror of one English page; it never defines separate behavior,
support, configuration, limits, or release semantics.

## Locale tree

Keep translations in `docs/i18n/<locale>/<canonical-source-path>`. For example,
the Simplified Chinese mirror of `README.md` belongs at
`docs/i18n/zh-Hans/README.md`. Use a stable BCP 47 locale such as `zh-Hans` or
`pt-BR`.

Each locale must have `docs/i18n/<locale>/manifest.tsv` with this header:

```text
source	status
```

Each following row uses the canonical source path and the status `reviewed`.
The manifest is the locale scope. A translated file outside the manifest fails
the parity gate, and a manifest row without its file also fails.

## Source binding

A translation starts with front matter that repeats the English contract and
binds to the exact English content revision:

```text
---
doc-contract: preview-0.2.0
doc-channel: default-branch
doc-source: README.md
source-sha256: 64-lowercase-hex-characters
translation-status: reviewed
---
```

The `source-sha256` value is the SHA-256 digest of the English page after its
front matter. This makes a source-content change detectable even when the Git
commit changes for an unrelated reason.

## Update workflow

1. Change the English source and its tests or gates first.
2. Change the translated meaning in the same coherent change.
3. Recompute `source-sha256` from the final English body.
4. Run `python3 tools/oss_docs_translation_gate.py`.
5. Have a reviewer who knows the source language confirm that behavior,
   refusal, support, security, release, and identity statements are unchanged.

A source edit without the matching translation revision fails as stale. A file
without a manifest row fails as unmanifested. A manifest row without a file
fails as missing. Contract metadata that differs from the English source also
fails.

## Review boundary

Do not translate code, public identifiers, properties, refusal codes, file
formats, CLI values, URLs, or command syntax. Keep examples runnable and keep
the capability matrix as the support boundary. Translations must pass the same
identity, local-path, predecessor-identity, content-boundary, and release
visibility rules as canonical English documentation.

No locale is shipped in the `0.2.0` source preview. When a locale is started,
begin with a small manifest of reviewed pages rather than an incomplete mirror
that implies complete coverage.
