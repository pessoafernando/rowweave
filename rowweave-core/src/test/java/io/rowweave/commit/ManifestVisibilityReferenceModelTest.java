package io.rowweave.commit;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Minimal reference model and exhaustive crash cuts for publication across two to five members. */
class ManifestVisibilityReferenceModelTest {

    @Test
    void directPathsHaveAnUnavoidablePartialCrashWindow() {
        for (int members = 2; members <= 5; members++) {
            Map<String, String> oldGeneration = generation(members, "old");
            Map<String, String> newGeneration = generation(members, "new");
            for (int publishedMembers = 1; publishedMembers < members; publishedMembers++) {
                Map<String, String> visible = new LinkedHashMap<>(oldGeneration);
                for (int i = 0; i < publishedMembers; i++) {
                    visible.put(member(i), newGeneration.get(member(i)));
                }
                assertFalse(visible.equals(oldGeneration));
                assertFalse(visible.equals(newGeneration),
                        "renaming path by path necessarily exposes a mixed old and new version when it crashes at entry N");
            }
        }
    }

    @Test
    void manifestReaderSeesOnlyOldOrCompleteNewAtEveryCrashCut() {
        for (int members = 2; members <= 5; members++) {
            Model model = new Model(generation(members, "old"));
            Map<String, String> next = generation(members, "new");

            assertEquals(generation(members, "old"), model.read());
            for (Map.Entry<String, String> member : next.entrySet()) {
                model.prepare(member.getKey(), member.getValue());
                assertEquals(generation(members, "old"), model.read(),
                        "crashing at any data, staging or manifest-temp boundary must still read only the old generation before the commit point");
            }
            model.writeManifestTemp();
            assertEquals(generation(members, "old"), model.read());
            model.publishManifest(1L);
            assertEquals(next, model.read(), "publishing one manifest switches to the complete new generation in a single step");
            model.recordOutcome();
            assertEquals(next, model.read(), "an outcome ACK is not the visibility commit point");
        }
    }

    @Test
    void incompleteOrStaleGenerationCannotBecomeVisible() {
        Model incomplete = new Model(generation(3, "old"));
        incomplete.prepare("m0", "new-0");
        assertThrows(IllegalStateException.class, () -> incomplete.publishManifest(1L));

        Model fenced = new Model(generation(2, "old"));
        fenced.prepareAll(generation(2, "generation-2"));
        fenced.writeManifestTemp();
        fenced.publishManifest(2L);
        fenced.prepareAll(generation(2, "late-generation-1"));
        fenced.writeManifestTemp();
        fenced.publishManifest(1L);

        assertEquals(generation(2, "generation-2"), fenced.read(),
                "a late manifest with a lower generation must not hide a higher committed version");
        assertFalse(fenced.outcomeRecorded, "an outcome ACK takes no part in the visibility choice of the highest manifest");
    }

    private static Map<String, String> generation(int members, String prefix) {
        Map<String, String> values = new LinkedHashMap<>();
        for (int i = 0; i < members; i++) {
            values.put(member(i), prefix + "-" + i);
        }
        return values;
    }

    private static String member(int index) {
        return "m" + index;
    }

    private static final class Model {
        private final int memberCount;
        private final Map<Long, Map<String, String>> manifests = new LinkedHashMap<>();
        private final Map<String, String> prepared = new LinkedHashMap<>();
        private boolean manifestTempDurable;
        private boolean outcomeRecorded;

        private Model(Map<String, String> initial) {
            this.memberCount = initial.size();
            manifests.put(0L, new LinkedHashMap<>(initial));
        }

        private void prepare(String member, String value) {
            prepared.put(member, value);
            manifestTempDurable = false;
        }

        private void prepareAll(Map<String, String> values) {
            prepared.clear();
            prepared.putAll(values);
            manifestTempDurable = false;
        }

        private void writeManifestTemp() {
            if (prepared.size() != memberCount) {
                throw new IllegalStateException("incomplete member set");
            }
            manifestTempDurable = true;
        }

        private void publishManifest(long generation) {
            if (prepared.size() != memberCount || !manifestTempDurable) {
                throw new IllegalStateException("manifest is not durable and complete");
            }
            manifests.put(generation, new LinkedHashMap<>(prepared));
            prepared.clear();
            manifestTempDurable = false;
        }

        private Map<String, String> read() {
            long highest = Long.MIN_VALUE;
            Map<String, String> visible = null;
            for (Map.Entry<Long, Map<String, String>> manifest : manifests.entrySet()) {
                if (manifest.getKey() > highest) {
                    highest = manifest.getKey();
                    visible = manifest.getValue();
                }
            }
            return new LinkedHashMap<>(visible);
        }

        private void recordOutcome() {
            outcomeRecorded = true;
        }
    }
}
