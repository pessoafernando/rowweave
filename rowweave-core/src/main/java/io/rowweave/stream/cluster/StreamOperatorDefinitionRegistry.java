package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamStateDescriptor;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

/**
 * Exhaustive node-local execution registry.
 *
 * <p>The registry is deliberately separate from graph admission: adding an
 * opcode without classifying its task lifecycle fails class initialization.</p>
 */
final class StreamOperatorDefinitionRegistry {
    enum Lifecycle {
        UNIFIED_TASK
    }

    enum StatePartitioning {
        NONE,
        KEY_GROUP,
        TASK_LOCAL,
        KEY_GROUP_AND_TASK_LOCAL
    }

    enum TaskLocalStateContract {
        EVENT_TIME_ASSIGNMENT,
        ASYNC_WAIT,
        MULTI_INPUT_GATE,
        BROADCAST_DIGEST
    }

    enum KeyedStateMigrationContract {
        NONE,
        INDEPENDENT_DESCRIPTORS,
        TEMPORAL_JOIN_SNAPSHOT
    }

    static final class Definition {
        private final StreamOperatorCode opcode;
        private final Lifecycle lifecycle;
        private final StatePartitioning statePartitioning;
        private final DescriptorSource descriptorSource;
        private final List<TaskLocalStateContract> taskLocalStateContracts;
        private final KeyedStateMigrationContract keyedStateMigration;

        private Definition(StreamOperatorCode opcode, Lifecycle lifecycle,
                StatePartitioning statePartitioning,
                DescriptorSource descriptorSource,
                List<TaskLocalStateContract> taskLocalStateContracts,
                KeyedStateMigrationContract keyedStateMigration) {
            this.opcode = opcode;
            this.lifecycle = lifecycle;
            this.statePartitioning = statePartitioning;
            this.descriptorSource = descriptorSource;
            this.taskLocalStateContracts = taskLocalStateContracts;
            this.keyedStateMigration = keyedStateMigration;
        }

        StreamOperatorCode opcode() { return opcode; }
        Lifecycle lifecycle() { return lifecycle; }
        StatePartitioning statePartitioning() { return statePartitioning; }
        KeyedStateMigrationContract keyedStateMigration() {
            return keyedStateMigration;
        }
        List<TaskLocalStateContract> taskLocalStateContracts(
                PortableStreamNode node) {
            if (node == null || node.opcode() != opcode) {
                throw new IllegalArgumentException(
                        "Stream node differs from its execution definition");
            }
            if (descriptorSource != DescriptorSource.PROCESS) {
                return taskLocalStateContracts;
            }
            StreamProcessBinding process = (StreamProcessBinding) node.binding();
            if (process.broadcastState() == null) {
                return taskLocalStateContracts;
            }
            List<TaskLocalStateContract> contracts = new ArrayList<>(
                    taskLocalStateContracts);
            contracts.add(TaskLocalStateContract.BROADCAST_DIGEST);
            return Collections.unmodifiableList(contracts);
        }

        List<StreamStateDescriptor> replicatedStateDescriptors(
                PortableStreamNode node) {
            if (node == null || node.opcode() != opcode) {
                throw new IllegalArgumentException(
                        "Stream node differs from its execution definition");
            }
            if (descriptorSource != DescriptorSource.PROCESS) {
                return Collections.emptyList();
            }
            StreamProcessBinding process = (StreamProcessBinding) node.binding();
            if (process.broadcastState() == null) {
                return Collections.emptyList();
            }
            return Collections.singletonList(
                    StreamKeyedProcessOperatorTask.broadcastStateDescriptor(
                            process.broadcastState()));
        }

        List<StreamStateDescriptor> keyedStateDescriptors(
                PortableStreamNode node) {
            if (node == null || node.opcode() != opcode) {
                throw new IllegalArgumentException(
                        "Stream node differs from its execution definition");
            }
            if (descriptorSource == DescriptorSource.NODE) {
                return node.stateDescriptors();
            }
            if (descriptorSource == DescriptorSource.PROCESS) {
                StreamProcessBinding process =
                        (StreamProcessBinding) node.binding();
                List<StreamStateDescriptor> descriptors = new ArrayList<>();
                for (io.rowweave.stream.contract
                        .StreamManagedStateDescriptor state
                        : process.managedStates()) {
                    descriptors.add(state.state());
                }
                for (io.rowweave.stream.contract.StreamTimerDescriptor timer
                        : process.timerFamilies()) {
                    descriptors.add(StreamKeyedProcessOperatorTask
                            .timerStateDescriptor(timer));
                }
                return Collections.unmodifiableList(descriptors);
            }
            return Collections.emptyList();
        }
    }

    private enum DescriptorSource { NONE, NODE, PROCESS }

    private static final Map<StreamOperatorCode, Definition> DEFINITIONS;

    static {
        EnumMap<StreamOperatorCode, Definition> definitions =
                new EnumMap<>(StreamOperatorCode.class);
        unified(definitions, StatePartitioning.NONE, DescriptorSource.NONE,
                StreamOperatorCode.SOURCE, StreamOperatorCode.MAP,
                StreamOperatorCode.FILTER, StreamOperatorCode.KEY_BY,
                StreamOperatorCode.SINK, StreamOperatorCode.CALC,
                StreamOperatorCode.FLAT_MAP);
        unified(definitions, StatePartitioning.TASK_LOCAL,
                DescriptorSource.NONE,
                Collections.singletonList(
                        TaskLocalStateContract.MULTI_INPUT_GATE),
                StreamOperatorCode.UNION);
        unified(definitions, StatePartitioning.KEY_GROUP,
                DescriptorSource.NODE,
                StreamOperatorCode.KEYED_AGGREGATE,
                StreamOperatorCode.EVENT_TIME_WINDOW,
                StreamOperatorCode.GENERIC_WINDOW);
        unified(definitions, StatePartitioning.KEY_GROUP,
                DescriptorSource.NODE,
                KeyedStateMigrationContract.TEMPORAL_JOIN_SNAPSHOT,
                StreamOperatorCode.TEMPORAL_JOIN);
        unified(definitions, StatePartitioning.TASK_LOCAL,
                DescriptorSource.NONE,
                Collections.singletonList(
                        TaskLocalStateContract.EVENT_TIME_ASSIGNMENT),
                StreamOperatorCode.ASSIGN_EVENT_TIME);
        unified(definitions, StatePartitioning.TASK_LOCAL,
                DescriptorSource.NONE,
                Collections.singletonList(TaskLocalStateContract.ASYNC_WAIT),
                StreamOperatorCode.ASYNC_WAIT);
        unified(definitions, StatePartitioning.KEY_GROUP_AND_TASK_LOCAL,
                DescriptorSource.PROCESS,
                StreamOperatorCode.KEYED_PROCESS);
        unified(definitions, StatePartitioning.KEY_GROUP_AND_TASK_LOCAL,
                DescriptorSource.PROCESS,
                Collections.singletonList(
                        TaskLocalStateContract.MULTI_INPUT_GATE),
                StreamOperatorCode.KEYED_CO_PROCESS);
        if (!definitions.keySet().equals(
                EnumSet.allOf(StreamOperatorCode.class))) {
            throw new ExceptionInInitializerError(
                    "every Stream opcode requires one execution definition");
        }
        DEFINITIONS = Collections.unmodifiableMap(definitions);
    }

    private StreamOperatorDefinitionRegistry() { }

    static Definition definition(StreamOperatorCode opcode) {
        if (opcode == null) {
            throw new IllegalArgumentException("Stream opcode is required");
        }
        Definition definition = DEFINITIONS.get(opcode);
        if (definition == null) {
            throw new IllegalStateException(
                    "Stream opcode has no execution definition: " + opcode);
        }
        return definition;
    }

    static Set<StreamOperatorCode> registeredOpcodes() {
        return Collections.unmodifiableSet(EnumSet.copyOf(DEFINITIONS.keySet()));
    }

    static Set<StreamOperatorCode> legacyShapeAdapterOpcodes() {
        EnumSet<StreamOperatorCode> legacy =
                EnumSet.noneOf(StreamOperatorCode.class);
        return Collections.unmodifiableSet(legacy);
    }

    private static void unified(
            Map<StreamOperatorCode, Definition> definitions,
            StatePartitioning statePartitioning,
            DescriptorSource descriptorSource,
            StreamOperatorCode... opcodes) {
        unified(definitions, statePartitioning, descriptorSource,
                Collections.emptyList(), opcodes);
    }

    private static void unified(
            Map<StreamOperatorCode, Definition> definitions,
            StatePartitioning statePartitioning,
            DescriptorSource descriptorSource,
            KeyedStateMigrationContract migration,
            StreamOperatorCode... opcodes) {
        for (StreamOperatorCode opcode : opcodes) {
            register(definitions, opcode, Lifecycle.UNIFIED_TASK,
                    statePartitioning, descriptorSource,
                    Collections.emptyList(), migration);
        }
    }

    private static void unified(
            Map<StreamOperatorCode, Definition> definitions,
            StatePartitioning statePartitioning,
            DescriptorSource descriptorSource,
            List<TaskLocalStateContract> taskLocalStateContracts,
            StreamOperatorCode... opcodes) {
        for (StreamOperatorCode opcode : opcodes) {
            register(definitions, opcode, Lifecycle.UNIFIED_TASK,
                    statePartitioning, descriptorSource,
                    taskLocalStateContracts,
                    statePartitioning == StatePartitioning.KEY_GROUP
                            || statePartitioning
                            == StatePartitioning.KEY_GROUP_AND_TASK_LOCAL
                            ? KeyedStateMigrationContract
                            .INDEPENDENT_DESCRIPTORS
                            : KeyedStateMigrationContract.NONE);
        }
    }

    private static void register(
            Map<StreamOperatorCode, Definition> definitions,
            StreamOperatorCode opcode, Lifecycle lifecycle,
            StatePartitioning statePartitioning,
            DescriptorSource descriptorSource,
            List<TaskLocalStateContract> taskLocalStateContracts,
            KeyedStateMigrationContract keyedStateMigration) {
        if (definitions.put(opcode, new Definition(
                opcode, lifecycle, statePartitioning,
                descriptorSource, Collections.unmodifiableList(
                new ArrayList<>(taskLocalStateContracts)),
                keyedStateMigration)) != null) {
            throw new ExceptionInInitializerError(
                    "duplicate Stream opcode execution definition: " + opcode);
        }
    }
}
