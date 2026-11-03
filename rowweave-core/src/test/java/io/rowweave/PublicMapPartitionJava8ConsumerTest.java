package io.rowweave;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** an external Java 8 source consumes only the public map-partition API. */
class PublicMapPartitionJava8ConsumerTest {
    @Test
    void publicApiCompilesToJava8AndExecutes(@TempDir Path temp)
            throws Exception {
        Path source = temp.resolve(
                "src/external/MapPartitionJava8Consumer.java");
        Path classes = Files.createDirectories(temp.resolve("classes"));
        Files.createDirectories(source.getParent());
        Files.write(source, source().getBytes(StandardCharsets.UTF_8));
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertTrue(compiler != null, "a JDK compiler is required");
        assertEquals(0, compiler.run(null, null, null,
                "-source", "8", "-target", "8",
                "-classpath", System.getProperty("java.class.path"),
                "-d", classes.toString(), source.toString()));
        byte[] classBytes = Files.readAllBytes(classes.resolve(
                "external/MapPartitionJava8Consumer.class"));
        int major = ((classBytes[6] & 0xff) << 8)
                | (classBytes[7] & 0xff);
        assertEquals(52, major);
        try (URLClassLoader loader = new URLClassLoader(
                new java.net.URL[]{classes.toUri().toURL()},
                getClass().getClassLoader())) {
            Object result = loader.loadClass(
                            "external.MapPartitionJava8Consumer")
                    .getMethod("verify").invoke(null);
            assertEquals("rows=1,bytes=256,value=7", result);
        }
    }

    private static String source() {
        return "package external;\n"
                + "import java.util.Collections;\n"
                + "import io.rowweave.Collector;\n"
                + "import io.rowweave.DataSet;\n"
                + "import io.rowweave.DataType;\n"
                + "import io.rowweave.Field;\n"
                + "import io.rowweave.MapPartitionEmissionLimit;\n"
                + "import io.rowweave.MapPartitionFunction;\n"
                + "import io.rowweave.PartitionDataRowCursor;\n"
                + "import io.rowweave.DataSchema;\n"
                + "import io.rowweave.DataRow;\n"
                + "public final class MapPartitionJava8Consumer {\n"
                + "  public static String verify() {\n"
                + "    final DataSchema meta=new DataSchema(new Field(\"id\",DataType.LongType));\n"
                + "    final DataRow input=new DataRow(1); input.set(0,7L);\n"
                + "    final boolean[] consumed={false}; final long[] output={-1L};\n"
                + "    MapPartitionFunction fn=new MapPartitionFunction(){\n"
                + "      public DataSchema getResultSchema(){return meta;}\n"
                + "      public void mapPartition(PartitionDataRowCursor rows,Collector out){\n"
                + "        DataRow row=rows.next(); if(row!=null) out.collect(row.copy());\n"
                + "      }};\n"
                + "    PartitionDataRowCursor rows=new PartitionDataRowCursor(){public DataRow next(){\n"
                + "      if(consumed[0]) return null; consumed[0]=true; return input; }};\n"
                + "    fn.mapPartition(rows,new Collector(){public void collect(DataRow row){\n"
                + "      output[0]=row.getLong(0); }});\n"
                + "    MapPartitionEmissionLimit limit=new MapPartitionEmissionLimit(1L,256L);\n"
                + "    DataSet.source(meta,Collections.<DataRow>emptyList()).mapPartition(fn,limit);\n"
                + "    return \"rows=\"+limit.maxOutputRowsPerPartition()+\",bytes=\""
                + "+limit.maxOutputBytesPerPartition()+\",value=\"+output[0];\n"
                + "  }\n"
                + "}\n";
    }
}
