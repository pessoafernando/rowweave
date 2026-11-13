---
doc-contract: preview-0.2.0
doc-channel: default-branch
---
# Third-party notices

RowWeave is licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE), [NOTICE](NOTICE)). This file records the third-party artifacts the build resolves, the license each one declares, and which of them are redistributed inside a published image.

The file is generated. Regenerate it from the build with:

```bash
tree="$PWD/rowweave-dependency-tree.txt"
mvn -o -B -ntp dependency:tree > "$tree"
python3 tools/oss_third_party_notices.py "$tree" \
    > THIRD-PARTY-NOTICES.md
```

Resolved third-party coordinates: 77. 35 have their compiled classes redistributed inside a connector image, 34 reach your application as ordinary Maven dependencies, and 8 are used only to build and test RowWeave.

## Redistributed inside a published image

These dependencies ship compiled classes inside a connector image, so their license terms travel with the image instead of being fetched from Maven.

| Artifact | Version | Declared license | License URL |
|---|---|---|---|
| `com.fasterxml.jackson.core:jackson-core` | 2.23.0 | The Apache Software License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.checkerframework:checker-qual` | 3.55.1 | The MIT License | https://opensource.org/licenses/MIT |
| `org.postgresql:postgresql` | 42.7.13 | BSD-2-Clause | https://jdbc.postgresql.org/about/license.html |
| `org.reactivestreams:reactive-streams` | 1.0.4 | MIT-0 | https://spdx.org/licenses/MIT-0.html |
| `org.slf4j:slf4j-api` | 2.0.20 | MIT | https://opensource.org/license/mit |
| `software.amazon.awssdk:annotations` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:arns` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:auth` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:aws-core` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:aws-query-protocol` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:aws-xml-protocol` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:checksums` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:checksums-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:crt-core` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:endpoints-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:http-auth` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:http-auth-aws` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:http-auth-aws-eventstream` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:http-auth-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:http-client-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:identity-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:json-utils` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:metrics-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:profiles` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:protocol-core` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:regions` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:retries` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:retries-spi` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:s3` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:sdk-core` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:third-party-jackson-core` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:url-connection-client` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:utils` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.awssdk:utils-lite` | 2.42.37 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |
| `software.amazon.eventstream:eventstream` | 1.0.1 | Apache License, Version 2.0 | https://aws.amazon.com/apache2.0 |

## Required at runtime by your application

Ordinary Maven dependencies: your application resolves them itself, so it receives each license text from the original artifact.

| Artifact | Version | Declared license | License URL |
|---|---|---|---|
| `com.google.errorprone:error_prone_annotations` | 2.50.0 | Apache 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `com.google.guava:failureaccess` | 1.0.3 | Apache License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `com.google.guava:guava` | 33.7.2-jre | Apache License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `com.google.guava:listenablefuture` | 9999.0-empty-to-avoid-conflict-with-guava | The Apache Software License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `com.google.j2objc:j2objc-annotations` | 3.1 | Apache License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `commons-io:commons-io` | 2.17.0 | Apache-2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `io.dropwizard.metrics:metrics-core` | 3.2.5 | Apache License 2.0 | http://www.apache.org/licenses/LICENSE-2.0.html |
| `io.netty:netty-buffer` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-codec` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-codec-http` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-common` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-handler` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-resolver` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-tcnative-boringssl-static` | 2.0.84.Final | The Apache Software License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `io.netty:netty-tcnative-classes` | 2.0.84.Final | The Apache Software License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `io.netty:netty-transport` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-transport-classes-epoll` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-transport-native-epoll` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `io.netty:netty-transport-native-unix-common` | 4.1.139.Final | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0 |
| `org.apache.curator:curator-client` | 5.9.0 | The Apache Software License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.apache.curator:curator-framework` | 5.9.0 | The Apache Software License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.apache.curator:curator-recipes` | 5.9.0 | The Apache Software License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.apache.curator:curator-test` | 5.9.0 | The Apache Software License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.apache.yetus:audience-annotations` | 0.12.0 | (not declared in its POM) | — |
| `org.apache.zookeeper:zookeeper` | 3.9.7 | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.apache.zookeeper:zookeeper-jute` | 3.9.7 | Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.apiguardian:apiguardian-api` | 1.1.2 | The Apache License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.jspecify:jspecify` | 1.0.1 | The Apache License, Version 2.0 | http://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.junit.jupiter:junit-jupiter-api` | 5.10.2 | Eclipse Public License v2.0 | https://www.eclipse.org/legal/epl-v20.html |
| `org.junit.platform:junit-platform-commons` | 1.10.2 | Eclipse Public License v2.0 | https://www.eclipse.org/legal/epl-v20.html |
| `org.opentest4j:opentest4j` | 1.3.0 | The Apache License, Version 2.0 | https://www.apache.org/licenses/LICENSE-2.0.txt |
| `org.ow2.asm:asm` | 9.11.1 | BSD-3-Clause | https://asm.ow2.io/license.html |
| `org.slf4j:slf4j-simple` | 2.0.20 | MIT | https://opensource.org/license/mit |
| `org.xerial.snappy:snappy-java` | 1.1.10.12 | Apache-2.0 | https://www.apache.org/licenses/LICENSE-2.0.html |

## Used only to build and test RowWeave

Test-scope artifacts are not part of any published artifact.

| Artifact | Version | Declared license | License URL |
|---|---|---|---|
| `ch.qos.logback:logback-classic` | 1.5.38 | EPL-2.0 + LGPL-2.1-only | https://www.eclipse.org/legal/epl-v20.html + https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html |
| `ch.qos.logback:logback-core` | 1.5.38 | EPL-2.0 + LGPL-2.1-only | https://www.eclipse.org/legal/epl-v20.html + https://www.gnu.org/licenses/old-licenses/lgpl-2.1.html |
| `com.h2database:h2` | 2.2.224 | (not declared in its POM) | — |
| `org.junit.jupiter:junit-jupiter` | 5.10.2 | Eclipse Public License v2.0 | https://www.eclipse.org/legal/epl-v20.html |
| `org.junit.jupiter:junit-jupiter-engine` | 5.10.2 | Eclipse Public License v2.0 | https://www.eclipse.org/legal/epl-v20.html |
| `org.junit.jupiter:junit-jupiter-params` | 5.10.2 | Eclipse Public License v2.0 | https://www.eclipse.org/legal/epl-v20.html |
| `org.junit.platform:junit-platform-engine` | 1.10.2 | Eclipse Public License v2.0 | https://www.eclipse.org/legal/epl-v20.html |
| `org.openjdk.jol:jol-core` | 0.17 | GNU General Public License (GPL), version 2, with the Classpath exception | http://openjdk.java.net/legal/gplv2+ce.html |

## License texts embedded in the published images

Established by reading each built image, so this describes the artifact rather than the dependency graph. Build the connector modules with `mvn -o -B -q package -DskipTests` before regenerating this file to populate the table.

| Image | Embedded license files |
|---|---|
| `rowweave-connector-postgres-cdc-0.2.0-runtime.jar` | `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/licenses/com.ongres.scram/scram-client-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.scram/scram-common-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/saslprep-2.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/stringprep-2.2/META-INF/LICENSE` |
| `rowweave-connector-postgres-cdc-0.2.1-runtime.jar` | `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/licenses/com.ongres.scram/scram-client-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.scram/scram-common-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/saslprep-2.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/stringprep-2.2/META-INF/LICENSE` |
| `rowweave-connector-postgres-cdc-0.2.0-deployment.jar` | `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/licenses/com.ongres.scram/scram-client-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.scram/scram-common-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/saslprep-2.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/stringprep-2.2/META-INF/LICENSE` |
| `rowweave-connector-postgres-cdc-0.2.1-deployment.jar` | `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/licenses/com.ongres.scram/scram-client-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.scram/scram-common-3.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/saslprep-2.2/META-INF/LICENSE`, `META-INF/licenses/com.ongres.stringprep/stringprep-2.2/META-INF/LICENSE` |
| `rowweave-connector-s3-0.2.0-runtime.jar` | `META-INF/FastDoubleParser-LICENSE`, `META-INF/FastDoubleParser-NOTICE`, `META-INF/FastDoubleParser-ThirdParty-LICENSE`, `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/NOTICE`, `META-INF/NOTICE.txt`, `META-INF/Schubfach-LICENSE`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-FastDoubleParser-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-NOTICE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.reactivestreams-reactive-streams-1.0.4-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.slf4j-slf4j-api-2.0.20-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-NOTICE.txt`, `META-INF/thirdparty-LICENSE` |
| `rowweave-connector-s3-0.2.1-runtime.jar` | `META-INF/FastDoubleParser-LICENSE`, `META-INF/FastDoubleParser-NOTICE`, `META-INF/FastDoubleParser-ThirdParty-LICENSE`, `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/NOTICE`, `META-INF/NOTICE.txt`, `META-INF/Schubfach-LICENSE`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-FastDoubleParser-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-NOTICE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.reactivestreams-reactive-streams-1.0.4-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.slf4j-slf4j-api-2.0.20-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-NOTICE.txt`, `META-INF/thirdparty-LICENSE` |
| `rowweave-connector-s3-0.2.0-deployment.jar` | `META-INF/FastDoubleParser-LICENSE`, `META-INF/FastDoubleParser-NOTICE`, `META-INF/FastDoubleParser-ThirdParty-LICENSE`, `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/NOTICE`, `META-INF/NOTICE.txt`, `META-INF/Schubfach-LICENSE`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-FastDoubleParser-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-NOTICE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.reactivestreams-reactive-streams-1.0.4-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.slf4j-slf4j-api-2.0.20-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-NOTICE.txt`, `META-INF/thirdparty-LICENSE` |
| `rowweave-connector-s3-0.2.1-deployment.jar` | `META-INF/FastDoubleParser-LICENSE`, `META-INF/FastDoubleParser-NOTICE`, `META-INF/FastDoubleParser-ThirdParty-LICENSE`, `META-INF/LICENSE`, `META-INF/LICENSE.txt`, `META-INF/NOTICE`, `META-INF/NOTICE.txt`, `META-INF/Schubfach-LICENSE`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-FastDoubleParser-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/com.fasterxml.jackson.core-jackson-core-2.22.3-NOTICE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.reactivestreams-reactive-streams-1.0.4-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/org.slf4j-slf4j-api-2.0.20-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-LICENSE.txt`, `META-INF/THIRD-PARTY-LICENSES/software.amazon.eventstream-eventstream-1.0.1-NOTICE.txt`, `META-INF/thirdparty-LICENSE` |

## License texts RowWeave adds to its images

A shaded image must carry the terms of the code inside it. Two situations break that, and both are handled here rather than by overriding the upstream entry, because overriding silently drops one document:

* the upstream artifact ships no license file, so shading would redistribute its classes with no terms attached;
* two artifacts put different text at the same `META-INF/` path, and only one survives shading.

RowWeave therefore keeps its own copy of each affected text under `src/main/resources/META-INF/THIRD-PARTY-LICENSES/`, named `group-artifact-version-LABEL.txt`. These paths cannot collide with an upstream entry.

| Module | File | Size | SHA-256 | Provenance verified against the build |
|---|---|---|---|---|
| `rowweave-connector-s3` | `com.fasterxml.jackson.core-jackson-core-2.22.3-FastDoubleParser-LICENSE.txt` | 3228 B | `59067508e44346bf` | byte-identical to META-INF/FastDoubleParser-LICENSE in jackson-core-2.22.3.jar |
| `rowweave-connector-s3` | `com.fasterxml.jackson.core-jackson-core-2.22.3-NOTICE.txt` | 1616 B | `e6aef1c0f4cf5b0f` | byte-identical to META-INF/NOTICE in jackson-core-2.22.3.jar |
| `rowweave-connector-s3` | `org.reactivestreams-reactive-streams-1.0.4-LICENSE.txt` | 911 B | `96a3b2d45af72054` | the upstream jar ships no such file (looked for META-INF/LICENSE, META-INF/LICENSE.txt) |
| `rowweave-connector-s3` | `org.slf4j-slf4j-api-2.0.20-LICENSE.txt` | 1159 B | `6632f550c207be8d` | byte-identical to META-INF/LICENSE.txt in slf4j-api-2.0.20.jar |
| `rowweave-connector-s3` | `software.amazon.eventstream-eventstream-1.0.1-LICENSE.txt` | 11358 B | `cfc7749b96f63bd3` | the upstream jar ships no such file (looked for META-INF/LICENSE, META-INF/LICENSE.txt) |
| `rowweave-connector-s3` | `software.amazon.eventstream-eventstream-1.0.1-NOTICE.txt` | 98 B | `8429c8479d27bcb4` | the upstream jar ships no such file (looked for META-INF/NOTICE) |

Where each externally sourced text came from, so it can be re-verified without the build. A text recorded as byte-identical above needs no separate source, since it was read from the artifact itself.

- `org.reactivestreams:reactive-streams:1.0.4` LICENSE: https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.4/COPYING
- `software.amazon.eventstream:eventstream:1.0.1` LICENSE: https://github.com/awslabs/aws-eventstream-java/blob/master/LICENSE
- `software.amazon.eventstream:eventstream:1.0.1` NOTICE: https://github.com/awslabs/aws-eventstream-java/blob/master/NOTICE

The two `com.fasterxml.jackson.core` entries are the documents that shading drops from `jackson-core` because `software.amazon.awssdk:third-party-jackson-core` claims the same `META-INF/NOTICE` and `META-INF/FastDoubleParser-LICENSE` paths with its own text. Both originals are preserved here.

`org.slf4j:slf4j-api` and `org.reactivestreams` classes are present in the S3 runtime image; the signed deployment image deliberately strips `org/slf4j/`, so that one text travels with the runtime image only. The directory itself passes into both images untouched.


## Mixed and copyleft-adjacent licenses

- Logback is dual-licensed EPL-1.0 or LGPL-2.1. RowWeave uses it only as a test logging binding, and you may choose either license.
- H2 is dual-licensed MPL-2.0 or EPL-1.0, and jol-core is GPLv2 with the Classpath exception. Both are test-scope and reach no published artifact.
- The PostgreSQL JDBC driver is BSD-2-Clause and carries license texts for its own shaded dependencies under `META-INF/licenses/`.
- ASM is BSD-3-Clause, Checker Framework qualifiers are MIT, and reactive streams is MIT-0, which drops the attribution clause.
- Artifacts whose POM declares no license block are labelled as such; RowWeave does not change their terms.

---

Generated by `tools/oss_third_party_notices.py` on 2026-11-13 from `rowweave_tree.log` and Maven-resolved POM metadata.
