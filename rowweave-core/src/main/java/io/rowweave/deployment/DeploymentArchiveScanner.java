package io.rowweave.deployment;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/** Strict single-disk, non-ZIP64 parser over the already verified private image. */
final class DeploymentArchiveScanner {
    static final int MAX_ENTRIES = 100_000;
    static final int MAX_CLASSES = 50_000;
    static final long MAX_ENTRY_BYTES = 16L * 1024L * 1024L;
    static final long MAX_EXPANDED_BYTES = 256L * 1024L * 1024L;
    static final long MAX_COMPRESSION_RATIO = 100L;

    private static final long LOCAL = 0x04034b50L;
    private static final long CENTRAL = 0x02014b50L;
    private static final long EOCD = 0x06054b50L;
    private static final long DESCRIPTOR = 0x08074b50L;

    private DeploymentArchiveScanner() {
    }

    static Index scan(byte[] image, DeploymentMemoryBudget budget) {
        if (image == null || budget == null || image.length < 22) {
            throw format("archive is too short");
        }
        int eocd = image.length - 22;
        if (u32(image, eocd) != EOCD || u16(image, eocd + 20) != 0) {
            throw format("EOCD must be the exact final record without a comment");
        }
        int disk = u16(image, eocd + 4);
        int centralDisk = u16(image, eocd + 6);
        int diskEntries = u16(image, eocd + 8);
        int entryCount = u16(image, eocd + 10);
        long centralSize = u32(image, eocd + 12);
        long centralOffset = u32(image, eocd + 16);
        if (disk != 0 || centralDisk != 0 || diskEntries != entryCount
                || entryCount == 0xffff || centralSize == 0xffffffffL
                || centralOffset == 0xffffffffL || entryCount > MAX_ENTRIES) {
            throw format("split/ZIP64/count archive is unsupported");
        }
        if (centralOffset + centralSize != eocd || centralOffset > Integer.MAX_VALUE
                || centralSize > Integer.MAX_VALUE) {
            throw format("central directory boundary mismatch");
        }

        long indexCharge = Math.addExact(512L + 256L * entryCount, 3L * centralSize);
        DeploymentMemoryBudget.Lease indexLease = budget.reserve(indexCharge);
        try {
            return parse(image, budget, entryCount, (int) centralOffset,
                    (int) (centralOffset + centralSize), indexLease);
        } catch (RuntimeException | Error failure) {
            indexLease.close();
            throw failure;
        }
    }

    private static Index parse(byte[] image, DeploymentMemoryBudget budget, int entryCount,
                               int centralStart, int centralEnd,
                               DeploymentMemoryBudget.Lease indexLease) {
        List<Record> records = new ArrayList<>(entryCount);
        Set<String> names = new HashSet<>();
        Set<String> classes = new HashSet<>();
        long expandedTotal = 0L;
        int position = centralStart;
        for (int i = 0; i < entryCount; i++) {
            require(image, position, 46, centralEnd, "central header");
            if (u32(image, position) != CENTRAL) throw format("central header signature mismatch");
            int versionNeeded = u16(image, position + 6);
            int flags = u16(image, position + 8);
            int method = u16(image, position + 10);
            long crc = u32(image, position + 16);
            long compressed = u32(image, position + 20);
            long expanded = u32(image, position + 24);
            int nameLength = u16(image, position + 28);
            int extraLength = u16(image, position + 30);
            int commentLength = u16(image, position + 32);
            int startDisk = u16(image, position + 34);
            long localOffset = u32(image, position + 42);
            if (versionNeeded >= 45) throw format("ZIP64-capable entry version is unsupported");
            rejectFlags(flags, method);
            rejectMethod(method);
            if (compressed == 0xffffffffL || expanded == 0xffffffffL
                    || localOffset == 0xffffffffL || startDisk != 0 || commentLength != 0) {
                throw format("ZIP64/disk/comment entry is unsupported");
            }
            if (expanded > MAX_ENTRY_BYTES) {
                throw failure("DEPLOYMENT_ENTRY_LIMIT_EXCEEDED", "entry exceeds 16 MiB");
            }
            if (expanded > 0L && (compressed == 0L
                    || expanded > compressed * MAX_COMPRESSION_RATIO)) {
                throw failure("DEPLOYMENT_COMPRESSION_RATIO_EXCEEDED", "entry ratio exceeds 100");
            }
            int variable = checkedInt((long) nameLength + extraLength + commentLength,
                    "central variable fields");
            require(image, position + 46, variable, centralEnd, "central variable fields");
            String name = strictName(image, position + 46, nameLength);
            validateName(name);
            scanExtra(image, position + 46 + nameLength, extraLength);
            if (!names.add(name)) {
                throw failure("DEPLOYMENT_DUPLICATE_ENTRY", name);
            }
            String className = className(name);
            if (className != null && !classes.add(className)) {
                throw failure("DEPLOYMENT_DUPLICATE_CLASS", className);
            }
            expandedTotal = Math.addExact(expandedTotal, expanded);
            if (expandedTotal > MAX_EXPANDED_BYTES) {
                throw failure("DEPLOYMENT_ENTRY_LIMIT_EXCEEDED", "archive expanded bytes exceed limit");
            }
            if (classes.size() > MAX_CLASSES) {
                throw failure("DEPLOYMENT_ENTRY_LIMIT_EXCEEDED", "archive class count exceeds limit");
            }
            records.add(new Record(name, className, flags, method, crc,
                    checkedInt(compressed, "compressed size"), checkedInt(expanded, "expanded size"),
                    checkedInt(localOffset, "local offset"), position + 46, nameLength));
            position += 46 + variable;
        }
        if (position != centralEnd) throw format("central directory has trailing or missing records");

        for (Record record : records) validateLocal(image, record, centralStart);
        List<Record> localOrder = new ArrayList<>(records);
        localOrder.sort(Comparator.comparingInt(value -> value.localOffset));
        int expectedOffset = 0;
        for (Record record : localOrder) {
            if (record.localOffset != expectedOffset) {
                throw format("local records have gaps, overlap or preamble");
            }
            expectedOffset = record.localEnd;
        }
        if (expectedOffset != centralStart) {
            throw format("local records do not end at central directory");
        }

        Map<String, Entry> entries = new LinkedHashMap<>();
        for (Record record : records) {
            byte[] digest = expandAndDigest(image, record, budget);
            entries.put(record.name, new Entry(record.name, record.className,
                    record.method, record.dataOffset, record.compressedSize,
                    record.expandedSize, digest));
        }
        return new Index(Collections.unmodifiableMap(entries),
                Collections.unmodifiableSet(classes), expandedTotal, indexLease);
    }

    private static void validateLocal(byte[] image, Record record, int centralStart) {
        int offset = record.localOffset;
        require(image, offset, 30, centralStart, "local header");
        if (u32(image, offset) != LOCAL) throw format("local header signature mismatch");
        int versionNeeded = u16(image, offset + 4);
        int flags = u16(image, offset + 6);
        int method = u16(image, offset + 8);
        long crc = u32(image, offset + 14);
        long compressed = u32(image, offset + 18);
        long expanded = u32(image, offset + 22);
        int nameLength = u16(image, offset + 26);
        int extraLength = u16(image, offset + 28);
        if (versionNeeded >= 45 || flags != record.flags || method != record.method
                || nameLength != record.nameLength) {
            throw format("local/central entry metadata mismatch");
        }
        int variable = checkedInt((long) nameLength + extraLength, "local variable fields");
        require(image, offset + 30, variable, centralStart, "local variable fields");
        if (!sliceEquals(image, offset + 30, record.centralNameOffset, nameLength)) {
            throw format("local/central entry name mismatch");
        }
        scanExtra(image, offset + 30 + nameLength, extraLength);
        int dataOffset = offset + 30 + variable;
        int dataEnd = checkedInt((long) dataOffset + record.compressedSize, "entry data end");
        if (dataEnd > centralStart) throw format("entry data overlaps central directory");
        int localEnd = dataEnd;
        if ((flags & 8) != 0) {
            require(image, dataEnd, 12, centralStart, "data descriptor");
            int descriptor = descriptorBodyOffset(image, dataEnd, centralStart, record);
            if (descriptor < 0) {
                throw format("data descriptor does not match central directory");
            }
            localEnd = descriptor + 12;
            if (!zeroOrEqual(crc, record.crc)
                    || !zeroOrEqual(compressed, record.compressedSize)
                    || !zeroOrEqual(expanded, record.expandedSize)) {
                throw format("descriptor-mode local sizes conflict with central directory");
            }
        } else if (crc != record.crc || compressed != record.compressedSize
                || expanded != record.expandedSize) {
            throw format("local sizes do not match central directory");
        }
        record.dataOffset = dataOffset;
        record.localEnd = localEnd;
    }

    private static byte[] expandAndDigest(byte[] image, Record record,
                                          DeploymentMemoryBudget budget) {
        DeploymentMemoryBudget.Lease lease = budget.reserve(Math.max(1, record.expandedSize));
        try {
            byte[] expanded = new byte[record.expandedSize];
            expand(image, record.method, record.dataOffset,
                    record.compressedSize, record.expandedSize, expanded);
            CRC32 crc = new CRC32();
            crc.update(expanded);
            if (crc.getValue() != record.crc) throw integrity("entry CRC mismatch");
            try {
                return MessageDigest.getInstance("SHA-256").digest(expanded);
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
        } finally {
            lease.close();
        }
    }

    static ExpandedEntry expandVerified(byte[] image, Entry entry,
                                        DeploymentMemoryBudget budget) {
        DeploymentMemoryBudget.Lease lease = budget.reserve(Math.max(1, entry.expandedSize));
        try {
            byte[] expanded = new byte[entry.expandedSize];
            expand(image, entry.method, entry.dataOffset,
                    entry.compressedSize, entry.expandedSize, expanded);
            byte[] actual;
            try {
                actual = MessageDigest.getInstance("SHA-256").digest(expanded);
            } catch (NoSuchAlgorithmException impossible) {
                throw new IllegalStateException("SHA-256 unavailable", impossible);
            }
            if (!MessageDigest.isEqual(entry.sha256, actual)) {
                throw integrity("lazy entry digest mismatch");
            }
            return new ExpandedEntry(expanded, lease);
        } catch (RuntimeException | Error failure) {
            lease.close();
            throw failure;
        }
    }

    private static void expand(byte[] image, int method, int dataOffset,
                               int compressedSize, int expandedSize, byte[] target) {
        if (method == 0) {
            if (compressedSize != expandedSize) throw integrity("stored entry size mismatch");
            System.arraycopy(image, dataOffset, target, 0, target.length);
        } else {
            inflate(image, dataOffset, compressedSize, target);
        }
    }

    private static void inflate(byte[] image, int dataOffset, int compressedSize, byte[] target) {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(image, dataOffset, compressedSize);
            int offset = 0;
            if (target.length == 0) {
                byte[] probe = new byte[1];
                if (inflater.inflate(probe) != 0 || !inflater.finished()
                        || inflater.getRemaining() != 0) {
                    throw integrity("empty deflate entry has non-empty output or trailing input");
                }
                return;
            }
            while (offset < target.length) {
                int count = inflater.inflate(target, offset, target.length - offset);
                if (count > 0) {
                    offset += count;
                } else if (inflater.needsDictionary() || inflater.needsInput() || inflater.finished()) {
                    break;
                } else {
                    throw integrity("deflate stream made no progress");
                }
            }
            if (offset != target.length || !inflater.finished() || inflater.getRemaining() != 0) {
                throw integrity("deflate size/boundary mismatch");
            }
        } catch (DataFormatException failure) {
            throw new DeploymentValidationException(
                    "DEPLOYMENT_ENTRY_INTEGRITY_FAILED", "invalid deflate stream", failure);
        } finally {
            inflater.end();
        }
    }

    private static void rejectFlags(int flags, int method) {
        int allowed = (1 << 3) | (1 << 11);
        if (method == 8) allowed |= (1 << 1) | (1 << 2);
        if ((flags & ~allowed) != 0) {
            throw failure("DEPLOYMENT_ARCHIVE_FORBIDDEN", "encrypted/reserved archive flags");
        }
    }

    private static int descriptorBodyOffset(byte[] image, int dataEnd, int centralStart,
                                            Record record) {
        if (u32(image, dataEnd) == DESCRIPTOR && dataEnd <= centralStart - 16
                && descriptorMatches(image, dataEnd + 4, record)) {
            return dataEnd + 4;
        }
        return descriptorMatches(image, dataEnd, record) ? dataEnd : -1;
    }

    private static boolean descriptorMatches(byte[] image, int offset, Record record) {
        return offset >= 0 && offset <= image.length - 12
                && u32(image, offset) == record.crc
                && u32(image, offset + 4) == record.compressedSize
                && u32(image, offset + 8) == record.expandedSize;
    }

    private static void rejectMethod(int method) {
        if (method != 0 && method != 8) {
            throw failure("DEPLOYMENT_ARCHIVE_FORBIDDEN", "unsupported compression method");
        }
    }

    private static void scanExtra(byte[] image, int offset, int length) {
        int end = offset + length;
        int position = offset;
        while (position < end) {
            if (position > end - 4) throw format("truncated ZIP extra field");
            int id = u16(image, position);
            int size = u16(image, position + 2);
            position += 4;
            if (position > end - size) throw format("truncated ZIP extra field body");
            if (id == 0x0001 || id == 0x9901) {
                throw format("ZIP64/AES extra field is unsupported");
            }
            position += size;
        }
    }

    private static String strictName(byte[] image, int offset, int length) {
        if (length < 1) throw failure("DEPLOYMENT_ENTRY_NAME_INVALID", "empty entry name");
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(image, offset, length)).toString();
        } catch (CharacterCodingException failure) {
            throw new DeploymentValidationException(
                    "DEPLOYMENT_ENTRY_NAME_INVALID", "entry name is not strict UTF-8", failure);
        }
    }

    private static void validateName(String name) {
        if (name.indexOf('\0') >= 0 || name.indexOf('\\') >= 0 || name.startsWith("/")
                || name.endsWith("/") || name.startsWith("META-INF/versions/")) {
            throw failure(name.startsWith("META-INF/versions/")
                    ? "DEPLOYMENT_ARCHIVE_FORBIDDEN" : "DEPLOYMENT_ENTRY_NAME_INVALID", name);
        }
        String[] segments = name.split("/", -1);
        for (String segment : segments) {
            if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
                throw failure("DEPLOYMENT_ENTRY_NAME_INVALID", name);
            }
        }
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".jar") || lower.endsWith(".so") || lower.endsWith(".dll")
                || lower.endsWith(".dylib") || lower.endsWith(".jnilib")) {
            throw failure("DEPLOYMENT_ARCHIVE_FORBIDDEN", name);
        }
    }

    private static String className(String name) {
        if (!name.endsWith(".class")) return null;
        String binary = name.substring(0, name.length() - 6).replace('/', '.');
        if (binary.startsWith("java.") || binary.startsWith("javax.")
                || binary.startsWith("jdk.") || binary.startsWith("sun.")
                || (binary.startsWith("io.rowweave.")
                && !binary.startsWith("io.rowweave.connector."))) {
            throw failure("DEPLOYMENT_FORBIDDEN_NAMESPACE", binary);
        }
        return binary;
    }

    private static boolean sliceEquals(byte[] bytes, int left, int right, int length) {
        for (int i = 0; i < length; i++) if (bytes[left + i] != bytes[right + i]) return false;
        return true;
    }

    private static boolean zeroOrEqual(long value, long expected) {
        return value == 0L || value == expected;
    }

    private static int checkedInt(long value, String name) {
        if (value < 0L || value > Integer.MAX_VALUE) throw format(name + " overflows int");
        return (int) value;
    }

    private static void require(byte[] bytes, int offset, int length, int limit, String name) {
        if (offset < 0 || length < 0 || limit < 0 || limit > bytes.length || offset > limit - length) {
            throw format("truncated " + name);
        }
    }

    private static int u16(byte[] bytes, int offset) {
        return (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8);
    }

    private static long u32(byte[] bytes, int offset) {
        return (u16(bytes, offset) & 0xffffL) | ((long) u16(bytes, offset + 2) << 16);
    }

    private static DeploymentValidationException format(String message) {
        return failure("DEPLOYMENT_ZIP_FORMAT_INVALID", message);
    }

    private static DeploymentValidationException integrity(String message) {
        return failure("DEPLOYMENT_ENTRY_INTEGRITY_FAILED", message);
    }

    private static DeploymentValidationException failure(String code, String message) {
        return new DeploymentValidationException(code, message);
    }

    static final class Index implements AutoCloseable {
        final Map<String, Entry> entries;
        final Set<String> classNames;
        final long expandedBytes;
        private final DeploymentMemoryBudget.Lease lease;

        Index(Map<String, Entry> entries, Set<String> classNames, long expandedBytes,
              DeploymentMemoryBudget.Lease lease) {
            this.entries = entries;
            this.classNames = classNames;
            this.expandedBytes = expandedBytes;
            this.lease = lease;
        }

        @Override public void close() { lease.close(); }
    }

    static final class Entry {
        final String name;
        final String className;
        final int method;
        final int dataOffset;
        final int compressedSize;
        final int expandedSize;
        final byte[] sha256;

        Entry(String name, String className, int method, int dataOffset,
              int compressedSize, int expandedSize, byte[] sha256) {
            this.name = name;
            this.className = className;
            this.method = method;
            this.dataOffset = dataOffset;
            this.compressedSize = compressedSize;
            this.expandedSize = expandedSize;
            this.sha256 = sha256;
        }
    }

    static final class ExpandedEntry implements AutoCloseable {
        final byte[] bytes;
        private final DeploymentMemoryBudget.Lease lease;

        ExpandedEntry(byte[] bytes, DeploymentMemoryBudget.Lease lease) {
            this.bytes = bytes;
            this.lease = lease;
        }

        @Override public void close() { lease.close(); }
    }

    private static final class Record {
        final String name;
        final String className;
        final int flags;
        final int method;
        final long crc;
        final int compressedSize;
        final int expandedSize;
        final int localOffset;
        final int centralNameOffset;
        final int nameLength;
        int dataOffset;
        int localEnd;

        Record(String name, String className, int flags, int method, long crc,
               int compressedSize, int expandedSize, int localOffset,
               int centralNameOffset, int nameLength) {
            this.name = name;
            this.className = className;
            this.flags = flags;
            this.method = method;
            this.crc = crc;
            this.compressedSize = compressedSize;
            this.expandedSize = expandedSize;
            this.localOffset = localOffset;
            this.centralNameOffset = centralNameOffset;
            this.nameLength = nameLength;
        }
    }
}
