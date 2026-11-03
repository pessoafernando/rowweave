package io.rowweave.stream.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

/** Deterministic all-or-nothing Stream worker placement gate. */
public final class StreamWorkerCapabilityPool {
    private StreamWorkerCapabilityPool() { }

    public static StreamWorkerPlacement place(
            Collection<StreamWorkerCapabilityProfile> profiles,
            StreamTaskResourceRequest request) {
        if (profiles == null || profiles.isEmpty() || request == null) {
            throw new IllegalArgumentException(
                    "Stream worker profiles and request are required");
        }
        List<Candidate> candidates = new ArrayList<>();
        Set<String> workerIds = new HashSet<>();
        for (StreamWorkerCapabilityProfile profile : profiles) {
            if (profile == null) {
                throw new IllegalArgumentException(
                        "Stream worker profile must not be null");
            }
            if (!workerIds.add(profile.workerId())) {
                throw failure(
                        StreamClusterProtocolException.Code
                                .AMBIGUOUS_WORKER_INCARNATION,
                        "multiple incarnations exist for one Stream worker ID");
            }
            int capacity = capacity(profile, request);
            if (capacity > 0) {
                candidates.add(new Candidate(profile, capacity));
            }
        }
        Collections.sort(candidates, Comparator.comparing(
                candidate -> candidate.profile.workerId()));
        long capacity = 0L;
        for (Candidate candidate : candidates) {
            capacity += candidate.remaining;
        }
        if (capacity < request.taskCount()) {
            throw failure(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                    "eligible Stream worker capacity is insufficient");
        }

        List<StreamWorkerPlacement.Assignment> assignments = new ArrayList<>();
        while (assignments.size() < request.taskCount()) {
            for (Candidate candidate : candidates) {
                if (candidate.remaining == 0) {
                    continue;
                }
                StreamWorkerCapabilityProfile profile = candidate.profile;
                assignments.add(new StreamWorkerPlacement.Assignment(
                        assignments.size(), profile.workerId(),
                        profile.workerIncarnation()));
                candidate.remaining--;
                if (assignments.size() == request.taskCount()) {
                    break;
                }
            }
        }
        return new StreamWorkerPlacement(assignments);
    }

    /** Places a canonical heterogeneous task set with node-local eligibility. */
    public static StreamWorkerPlacement placeTasks(
            Collection<StreamWorkerCapabilityProfile> profiles,
            List<StreamTaskPlacementRequest> requests,
            BiPredicate<StreamTaskPlacementRequest,
                    StreamWorkerCapabilityProfile> eligibility) {
        if (profiles == null || profiles.isEmpty() || requests == null
                || requests.isEmpty() || eligibility == null) {
            throw new IllegalArgumentException(
                    "Stream worker profiles, tasks and eligibility are required");
        }
        List<TaskCandidate> candidates = new ArrayList<>();
        Set<String> workerIds = new HashSet<>();
        for (StreamWorkerCapabilityProfile profile : profiles) {
            if (profile == null) {
                throw new IllegalArgumentException(
                        "Stream worker profile must not be null");
            }
            if (!workerIds.add(profile.workerId())) {
                throw failure(StreamClusterProtocolException.Code
                                .AMBIGUOUS_WORKER_INCARNATION,
                        "multiple incarnations exist for one Stream worker ID");
            }
            candidates.add(new TaskCandidate(profile));
        }
        Collections.sort(candidates, Comparator.comparing(
                candidate -> candidate.profile.workerId()));
        Set<StreamTaskIdentity> identities = new HashSet<>();
        for (StreamTaskPlacementRequest request : requests) {
            if (request == null || !identities.add(request.identity())) {
                throw new IllegalArgumentException(
                        "Stream task placement identities must be unique");
            }
        }

        List<StreamTaskPlacementRequest> remaining = new ArrayList<>(requests);
        Map<StreamTaskIdentity, TaskCandidate> selectedByTask =
                new LinkedHashMap<>();
        while (!remaining.isEmpty()) {
            StreamTaskPlacementRequest request = null;
            int fewestEligible = Integer.MAX_VALUE;
            for (StreamTaskPlacementRequest candidateRequest : remaining) {
                int eligible = 0;
                for (TaskCandidate candidate : candidates) {
                    if (eligibility.test(candidateRequest, candidate.profile)
                            && candidate.fits(candidateRequest)) {
                        eligible++;
                    }
                }
                if (eligible < fewestEligible) {
                    request = candidateRequest;
                    fewestEligible = eligible;
                }
            }
            if (request == null || fewestEligible == 0) {
                StreamTaskIdentity missing = request == null
                        ? remaining.get(0).identity() : request.identity();
                throw failure(
                        StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                        "eligible Stream worker capacity is insufficient for "
                                + missing);
            }
            TaskCandidate selected = null;
            for (TaskCandidate candidate : candidates) {
                if (!eligibility.test(request, candidate.profile)
                        || !candidate.fits(request)) {
                    continue;
                }
                if (selected == null
                        || candidate.assignedTasks < selected.assignedTasks
                        || candidate.assignedTasks == selected.assignedTasks
                        && candidate.profile.workerId().compareTo(
                        selected.profile.workerId()) < 0) {
                    selected = candidate;
                }
            }
            selected.reserve(request);
            selectedByTask.put(request.identity(), selected);
            remaining.remove(request);
        }

        List<StreamWorkerPlacement.Assignment> assignments = new ArrayList<>();
        for (StreamTaskPlacementRequest request : requests) {
            TaskCandidate selected = selectedByTask.get(request.identity());
            assignments.add(new StreamWorkerPlacement.Assignment(
                    assignments.size(), request.identity().operatorUid(),
                    request.identity().subtaskIndex(),
                    selected.profile.workerId(),
                    selected.profile.workerIncarnation()));
        }
        return new StreamWorkerPlacement(assignments);
    }

    private static int capacity(
            StreamWorkerCapabilityProfile profile,
            StreamTaskResourceRequest request) {
        if (profile.controlProtocolVersion()
                < request.controlProtocolVersion()
                || profile.dataProtocolVersion()
                < request.dataProtocolVersion()
                || profile.maximumFrameBytes() < request.maximumFrameBytes()
                || !profile.capabilities().containsAll(
                        request.requiredCapabilities())) {
            return 0;
        }
        long residentCapacity = profile.maximumResidentBytes()
                / request.residentBytesPerTask();
        long threadCapacity = profile.maximumThreads()
                / request.threadsPerTask();
        long channelCapacity = profile.channelCapacity()
                / request.channelsPerTask();
        long result = Math.min(profile.taskSlots(), channelCapacity);
        result = Math.min(result, residentCapacity);
        result = Math.min(result, threadCapacity);
        return (int) Math.min(Integer.MAX_VALUE, result);
    }

    private static StreamClusterProtocolException failure(
            StreamClusterProtocolException.Code code, String message) {
        return new StreamClusterProtocolException(code, message);
    }

    private static final class Candidate {
        final StreamWorkerCapabilityProfile profile;
        int remaining;

        Candidate(StreamWorkerCapabilityProfile profile, int remaining) {
            this.profile = profile;
            this.remaining = remaining;
        }
    }

    private static final class TaskCandidate {
        final StreamWorkerCapabilityProfile profile;
        int assignedTasks;
        int channels;
        int fileDescriptors;
        long residentBytes;
        int threads;
        int timerCount;
        int windowCount;
        long checkpointBytes;
        long spillDiskBytes;

        TaskCandidate(StreamWorkerCapabilityProfile profile) {
            this.profile = profile;
        }

        boolean fits(StreamTaskPlacementRequest request) {
            return profile.controlProtocolVersion()
                            >= request.controlProtocolVersion()
                    && profile.dataProtocolVersion()
                            >= request.dataProtocolVersion()
                    && profile.maximumFrameBytes()
                            >= request.maximumFrameBytes()
                    && profile.capabilities().containsAll(
                            request.requiredCapabilities())
                    && add(assignedTasks, 1) <= profile.taskSlots()
                    && add(channels, request.channels())
                            <= profile.channelCapacity()
                    && add(fileDescriptors, request.fileDescriptorCount())
                            <= profile.maximumFileDescriptorCount()
                    && add(residentBytes, request.residentBytes())
                            <= profile.maximumResidentBytes()
                    && add(threads, request.threads())
                            <= profile.maximumThreads()
                    && add(timerCount, request.timerCount())
                            <= profile.maximumTimerCount()
                    && add(windowCount, request.windowCount())
                            <= profile.maximumWindowCount()
                    && add(checkpointBytes, request.checkpointBytes())
                            <= profile.maximumCheckpointBytes()
                    && add(spillDiskBytes, request.spillDiskBytes())
                            <= profile.maximumSpillDiskBytes();
        }

        void reserve(StreamTaskPlacementRequest request) {
            assignedTasks++;
            channels = Math.addExact(channels, request.channels());
            fileDescriptors = Math.addExact(
                    fileDescriptors, request.fileDescriptorCount());
            residentBytes = Math.addExact(
                    residentBytes, request.residentBytes());
            threads = Math.addExact(threads, request.threads());
            timerCount = Math.addExact(timerCount, request.timerCount());
            windowCount = Math.addExact(windowCount, request.windowCount());
            checkpointBytes = Math.addExact(
                    checkpointBytes, request.checkpointBytes());
            spillDiskBytes = Math.addExact(
                    spillDiskBytes, request.spillDiskBytes());
        }

        private static long add(long left, long right) {
            try {
                return Math.addExact(left, right);
            } catch (ArithmeticException overflow) {
                return Long.MAX_VALUE;
            }
        }

        private static int add(int left, int right) {
            try {
                return Math.addExact(left, right);
            } catch (ArithmeticException overflow) {
                return Integer.MAX_VALUE;
            }
        }
    }
}
