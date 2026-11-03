package io.rowweave.expr.codegen;

import io.rowweave.DataSchema;
import io.rowweave.expr.Expr;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * AST to bytecode generator: compiles a subset of {@link Expr} into a bytecode class implementing {@link io.rowweave.expr.CompiledExpr}.
 *
 * <p>Correctness policy, not negotiable: no leaf semantic is hand-written in bytecode. Each leaf calls a static method of
 * {@link io.rowweave.expr.ExprRuntime}, the same code the {@link io.rowweave.expr.ExprInterpreter} runs, so the two agree bit for bit and decimal semantics cannot drift.
 * Bytecode is responsible only for: eliminating the per-node instanceof dispatch and boxing overhead of the interpreter; and short-circuiting and/or plus the taken branch of case and cast.
 *
 * <p>Constants (BigDecimal literals, strings, cast target types, function names) are stored in the instance's {@code Object[] consts}, injected at construction and
 * fetched at eval time with {@code aload consts; iconst i; aaload}, which sidesteps the fact that ldc cannot push a BigDecimal.
 *
 * <p>After each node emits, exactly one {@code Object} (the value or null) is left on the stack top. eval returns that Object; test is Boolean.TRUE.equals(eval).
 */
public final class AstBytecodeGenerator implements Opcodes {

    private static final AtomicInteger SEQ = new AtomicInteger();
    private static final String COMPILED_EXPR = "io/rowweave/expr/CompiledExpr";
    private static final String ROW = "io/rowweave/DataRow";
    private static final String RUNTIME = "io/rowweave/expr/ExprRuntime";
    private static final String OBJ = "Ljava/lang/Object;";

    // Compile cache for the production path, to keep Metaspace from growing. Key = canonical expression string plus the DataSchema field layout signature
    // (the same string over a different schema has different field indices, so a compiled artifact cannot be reused; reading the wrong column would silently compute
    // a wrong value, which is why the layout is part of the key). Only successful artifacts are cached;
    private static final java.util.concurrent.ConcurrentHashMap<String, io.rowweave.expr.CompiledExpr> CACHE =
            new java.util.concurrent.ConcurrentHashMap<>();

    private final DataSchema meta;
    private final List<Object> consts = new ArrayList<>();
    // Local-slot allocator for the eval method. 0=this, 1=row, 2=params are fixed; CASE subject temporaries
    // claim slots from 3 upward and release on exit, so NESTED case-when never clobber each other (P0 fix).
    private int nextLocalSlot = 3;

    private AstBytecodeGenerator(DataSchema meta) {
        this.meta = meta;
    }

    /**
     * Compiles an AST into a CompiledExpr instance with caching: the same (canonical expr, schema field layout) reuses one compiled instance to keep
     * Metaspace from growing. An unsupported node throws {@link UnsupportedExprException} (the caller falls back to the interpreter and nothing is cached).
     */
    public static io.rowweave.expr.CompiledExpr compile(DataSchema meta, Expr root) {
        String key = root.toString() + "\u0000" + metaSignature(meta);
        io.rowweave.expr.CompiledExpr cached = CACHE.get(key);
        if (cached != null) {
            return cached;
        }
        // compile outside computeIfAbsent: UnsupportedExprException must propagate (not be cached/swallowed),
        // and a rare duplicate compile under race is harmless (both classes are equivalent; putIfAbsent dedups).
        io.rowweave.expr.CompiledExpr compiled = compileUncached(meta, root);
        io.rowweave.expr.CompiledExpr prev = CACHE.putIfAbsent(key, compiled);
        return prev != null ? prev : compiled;
    }

    private static io.rowweave.expr.CompiledExpr compileUncached(DataSchema meta, Expr root) {
        AstBytecodeGenerator g = new AstBytecodeGenerator(meta);
        String className = "io/rowweave/expr/codegen/Gen$Ast$" + SEQ.incrementAndGet();
        byte[] bytes = g.generate(className, root);
        try {
            Class<?> cls = new GeneratedClassLoader(AstBytecodeGenerator.class.getClassLoader())
                    .define(className.replace('/', '.'), bytes);
            return (io.rowweave.expr.CompiledExpr) cls.getConstructor(Object[].class)
                    .newInstance((Object) g.consts.toArray());
        } catch (ReflectiveOperationException e) {
            throw new UnsupportedExprException("failed to instantiate generated expr: " + e);
        }
    }

    /** Signature of the DataSchema field layout: name:typeOrdinal joined in field order. Field index semantics depend on position, so the order matters. */
    private static String metaSignature(DataSchema meta) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < meta.getFieldCount(); i++) {
            io.rowweave.Field f = meta.getField(i);
            sb.append(f.getName()).append(':').append(f.getDataType().ordinal()).append(';');
        }
        return sb.toString();
    }

    private byte[] generate(String className, Expr root) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        cw.visit(V1_8, ACC_PUBLIC | ACC_FINAL, className, null, "java/lang/Object",
                new String[]{COMPILED_EXPR});
        cw.visitField(ACC_PRIVATE | ACC_FINAL, "consts", "[Ljava/lang/Object;", null, null).visitEnd();

        emitConstructor(cw, className);
        emitEvalCore(cw, className, root); // Object eval(DataRow, Object[] params)
        emitEvalNoParams(cw, className);   // Object eval(DataRow) -> eval(row, NO_PARAMS)
        emitTest(cw, className, "(L" + ROW + ";)Z", false);
        emitTest(cw, className, "(L" + ROW + ";[Ljava/lang/Object;)Z", true);

        cw.visitEnd();
        return cw.toByteArray();
    }

    private void emitConstructor(ClassWriter cw, String className) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "([Ljava/lang/Object;)V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitFieldInsn(PUTFIELD, className, "consts", "[Ljava/lang/Object;");
        mv.visitInsn(RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    /** The core method: {@code Object eval(DataRow row, Object[] params)} - recursively emits the root node and returns the Object on the stack top directly. */
    private void emitEvalCore(ClassWriter cw, String className, Expr root) {
        // local slots: 0=this, 1=row, 2=params
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "eval",
                "(L" + ROW + ";[Ljava/lang/Object;)" + OBJ, null, null);
        mv.visitCode();
        emit(mv, className, root);
        mv.visitInsn(ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    private void emitEvalNoParams(ClassWriter cw, String className) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "eval", "(L" + ROW + ";)" + OBJ, null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitInsn(ACONST_NULL); // null params -> ExprRuntime.param guards length
        mv.visitMethodInsn(INVOKEVIRTUAL, className, "eval",
                "(L" + ROW + ";[Ljava/lang/Object;)" + OBJ, false);
        mv.visitInsn(ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    private void emitTest(ClassWriter cw, String className, String desc, boolean withParams) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "test", desc, null, null);
        mv.visitCode();
        mv.visitFieldInsn(GETSTATIC, "java/lang/Boolean", "TRUE", "Ljava/lang/Boolean;");
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        if (withParams) {
            mv.visitVarInsn(ALOAD, 2);
            mv.visitMethodInsn(INVOKEVIRTUAL, className, "eval",
                    "(L" + ROW + ";[Ljava/lang/Object;)" + OBJ, false);
        } else {
            mv.visitMethodInsn(INVOKEVIRTUAL, className, "eval", "(L" + ROW + ";)" + OBJ, false);
        }
        mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Boolean", "equals", "(Ljava/lang/Object;)Z", false);
        mv.visitInsn(IRETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    private int constIndex(Object value) {
        consts.add(value);
        return consts.size() - 1;
    }

    /** Pushes consts[i] (an Object) onto the stack. */
    private void loadConst(MethodVisitor mv, String className, Object value) {
        int i = constIndex(value);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitFieldInsn(GETFIELD, className, "consts", "[Ljava/lang/Object;");
        pushInt(mv, i);
        mv.visitInsn(AALOAD);
    }

    private static void pushInt(MethodVisitor mv, int v) {
        if (v >= -1 && v <= 5) {
            mv.visitInsn(ICONST_0 + v);
        } else if (v >= Byte.MIN_VALUE && v <= Byte.MAX_VALUE) {
            mv.visitIntInsn(BIPUSH, v);
        } else if (v >= Short.MIN_VALUE && v <= Short.MAX_VALUE) {
            mv.visitIntInsn(SIPUSH, v);
        } else {
            mv.visitLdcInsn(v);
        }
    }

    private static void pushBoolean(MethodVisitor mv, boolean b) {
        mv.visitFieldInsn(GETSTATIC, "java/lang/Boolean", b ? "TRUE" : "FALSE", "Ljava/lang/Boolean;");
    }

    /** Recursive emit: after execution, exactly one Object (value or null) is left on the stack top. */
    private void emit(MethodVisitor mv, String cn, Expr e) {
        if (e instanceof Expr.Aliased) {
            emit(mv, cn, ((Expr.Aliased) e).expr);
            return;
        }
        if (e instanceof Expr.NumberLit) {
            loadConst(mv, cn, io.rowweave.expr.ExprRuntime.numberLit(((Expr.NumberLit) e).value));
            return;
        }
        if (e instanceof Expr.StringLit) {
            loadConst(mv, cn, ((Expr.StringLit) e).value);
            return;
        }
        if (e instanceof Expr.Macro) {
            switch (((Expr.Macro) e).kind) {
                case NULL: mv.visitInsn(ACONST_NULL); return;
                case TRUE: pushBoolean(mv, true); return;
                case FALSE: pushBoolean(mv, false); return;
                default: throw new UnsupportedExprException("macro");
            }
        }
        if (e instanceof Expr.FieldRef) {
            emitField(mv, cn, (Expr.FieldRef) e);
            return;
        }
        if (e instanceof Expr.Param) {
            // ExprRuntime.param(params, index)
            mv.visitVarInsn(ALOAD, 2);
            pushInt(mv, ((Expr.Param) e).index);
            mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "param", "([Ljava/lang/Object;I)" + OBJ, false);
            return;
        }
        if (e instanceof Expr.UnaryOp) {
            emitUnary(mv, cn, (Expr.UnaryOp) e);
            return;
        }
        if (e instanceof Expr.BinaryOp) {
            emitBinary(mv, cn, (Expr.BinaryOp) e);
            return;
        }
        if (e instanceof Expr.IsNull) {
            Expr.IsNull n = (Expr.IsNull) e;
            emit(mv, cn, n.operand);
            mv.visitInsn(n.negated ? ICONST_1 : ICONST_0);
            mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "isNull", "(" + OBJ + "Z)" + OBJ, false);
            return;
        }
        if (e instanceof Expr.Like) {
            Expr.Like l = (Expr.Like) e;
            emit(mv, cn, l.value);
            emit(mv, cn, l.pattern);
            mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "like", "(" + OBJ + OBJ + ")" + OBJ, false);
            return;
        }
        if (e instanceof Expr.In) {
            emitIn(mv, cn, (Expr.In) e);
            return;
        }
        if (e instanceof Expr.Cast) {
            Expr.Cast c = (Expr.Cast) e;
            emit(mv, cn, c.operand);
            loadConst(mv, cn, c.targetType);
            mv.visitTypeInsn(CHECKCAST, "java/lang/String");
            mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "cast", "(" + OBJ + "Ljava/lang/String;)" + OBJ, false);
            return;
        }
        if (e instanceof Expr.Case) {
            emitCase(mv, cn, (Expr.Case) e);
            return;
        }
        if (e instanceof Expr.FuncCall) {
            emitFunc(mv, cn, (Expr.FuncCall) e);
            return;
        }
        throw new UnsupportedExprException("node not supported by codegen: " + e.getClass().getSimpleName());
    }

    private void emitField(MethodVisitor mv, String cn, Expr.FieldRef f) {
        int idx = meta.getFieldIndex(f.name);
        if (idx < 0) {
            // codegen does not support named-param fallback; let caller fall back to interpreter
            throw new UnsupportedExprException("unknown field (codegen): " + f.name);
        }
        mv.visitVarInsn(ALOAD, 1); // row
        pushInt(mv, idx);
        mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "field", "(L" + ROW + ";I)" + OBJ, false);
    }

    private void emitUnary(MethodVisitor mv, String cn, Expr.UnaryOp u) {
        emit(mv, cn, u.operand);
        if ("-".equals(u.op)) {
            mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "negate", "(" + OBJ + ")" + OBJ, false);
        } else if ("not".equals(u.op)) {
            mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "not", "(" + OBJ + ")" + OBJ, false);
        } else {
            throw new UnsupportedExprException("unary op: " + u.op);
        }
    }

    private void emitBinary(MethodVisitor mv, String cn, Expr.BinaryOp b) {
        switch (b.op) {
            case "and": emitShortCircuit(mv, cn, b, true); return;
            case "or": emitShortCircuit(mv, cn, b, false); return;
            case "xor":
                emit(mv, cn, b.left);
                emit(mv, cn, b.right);
                mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "xor", "(" + OBJ + OBJ + ")" + OBJ, false);
                return;
            case "+":
                emit(mv, cn, b.left);
                emit(mv, cn, b.right);
                mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "plus", "(" + OBJ + OBJ + ")" + OBJ, false);
                return;
            case "-": case "*": case "/": case "%":
                pushInt(mv, arithOpCode(b.op)); // op first (arith(int, Object, Object))
                emit(mv, cn, b.left);
                emit(mv, cn, b.right);
                mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "arith", "(I" + OBJ + OBJ + ")" + OBJ, false);
                return;
            default:
                emitComparison(mv, cn, b);
                return;
        }
    }

    /** AND/OR short circuit: with left FALSE (and) or TRUE (or) the value is already decided; otherwise right is computed and ExprRuntime.and/or combines the three-valued result. */
    private void emitShortCircuit(MethodVisitor mv, String cn, Expr.BinaryOp b, boolean isAnd) {
        emit(mv, cn, b.left);   // stack: L
        mv.visitInsn(DUP);      // L, L
        // check decisive value: and -> isFalse ; or -> isTrue
        mv.visitMethodInsn(INVOKESTATIC, RUNTIME, isAnd ? "isFalse" : "isTrue", "(" + OBJ + ")Z", false);
        Label notDecisive = new Label();
        mv.visitJumpInsn(IFEQ, notDecisive); // if !decisive -> compute right
        // decisive: pop L, push FALSE(and)/TRUE(or)
        mv.visitInsn(POP);
        pushBoolean(mv, !isAnd); // and-decisive => FALSE ; or-decisive => TRUE
        Label done = new Label();
        mv.visitJumpInsn(GOTO, done);
        mv.visitLabel(notDecisive);
        // stack: L ; compute R then combine
        emit(mv, cn, b.right);  // L, R
        mv.visitMethodInsn(INVOKESTATIC, RUNTIME, isAnd ? "and" : "or", "(" + OBJ + OBJ + ")" + OBJ, false);
        mv.visitLabel(done);
    }

    private static int arithOpCode(String op) {
        switch (op) {
            case "-": return io.rowweave.expr.ExprRuntime.OP_SUB;
            case "*": return io.rowweave.expr.ExprRuntime.OP_MUL;
            case "/": return io.rowweave.expr.ExprRuntime.OP_DIV;
            case "%": return io.rowweave.expr.ExprRuntime.OP_REM;
            default: throw new UnsupportedExprException("arith op: " + op);
        }
    }

    private void emitComparison(MethodVisitor mv, String cn, Expr.BinaryOp b) {
        int op;
        switch (b.op) {
            case "=": op = io.rowweave.expr.ExprRuntime.CMP_EQ; break;
            case "<>": case "!=": op = io.rowweave.expr.ExprRuntime.CMP_NE; break;
            case "<": op = io.rowweave.expr.ExprRuntime.CMP_LT; break;
            case "<=": op = io.rowweave.expr.ExprRuntime.CMP_LE; break;
            case ">": op = io.rowweave.expr.ExprRuntime.CMP_GT; break;
            case ">=": op = io.rowweave.expr.ExprRuntime.CMP_GE; break;
            default: throw new UnsupportedExprException("binary op: " + b.op);
        }
        pushInt(mv, op);
        emit(mv, cn, b.left);
        emit(mv, cn, b.right);
        mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "compare", "(I" + OBJ + OBJ + ")" + OBJ, false);
    }

    private void emitIn(MethodVisitor mv, String cn, Expr.In e) {
        emit(mv, cn, e.value);            // value
        // build Object[] items
        pushInt(mv, e.items.size());
        mv.visitTypeInsn(ANEWARRAY, "java/lang/Object");
        for (int i = 0; i < e.items.size(); i++) {
            mv.visitInsn(DUP);
            pushInt(mv, i);
            emit(mv, cn, e.items.get(i));
            mv.visitInsn(AASTORE);
        }
        mv.visitInsn(e.negated ? ICONST_1 : ICONST_0);
        mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "in", "(" + OBJ + "[Ljava/lang/Object;Z)" + OBJ, false);
    }

    private void emitCase(MethodVisitor mv, String cn, Expr.Case e) {
        // Claim a FRESH local slot for this case's subject so a nested case (in a WHEN/THEN) cannot clobber
        // an enclosing case's subject. Released after the case so sibling cases can reuse the slot.
        int subjectSlot = nextLocalSlot++;
        try {
            if (e.subject == null) {
                mv.visitInsn(ACONST_NULL);
            } else {
                emit(mv, cn, e.subject);
            }
            mv.visitVarInsn(ASTORE, subjectSlot);

            Label done = new Label();
            for (int i = 0; i < e.whens.size(); i++) {
                Label next = new Label();
                if (e.subject == null) {
                    // searched: when is predicate -> ExprRuntime.isTrue(when)
                    emit(mv, cn, e.whens.get(i));
                    mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "isTrue", "(" + OBJ + ")Z", false);
                } else {
                    // simple: ExprRuntime.caseEq(subject, when)
                    mv.visitVarInsn(ALOAD, subjectSlot);
                    emit(mv, cn, e.whens.get(i));
                    mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "caseEq", "(" + OBJ + OBJ + ")Z", false);
                }
                mv.visitJumpInsn(IFEQ, next); // not hit -> next branch
                emit(mv, cn, e.thens.get(i)); // hit -> push then-value
                mv.visitJumpInsn(GOTO, done);
                mv.visitLabel(next);
            }
            if (e.elseExpr == null) {
                mv.visitInsn(ACONST_NULL);
            } else {
                emit(mv, cn, e.elseExpr);
            }
            mv.visitLabel(done);
        } finally {
            nextLocalSlot--; // release the slot for sibling cases
        }
    }

    private void emitFunc(MethodVisitor mv, String cn, Expr.FuncCall f) {
        loadConst(mv, cn, f.name);
        mv.visitTypeInsn(CHECKCAST, "java/lang/String");
        pushInt(mv, f.args.size());
        mv.visitTypeInsn(ANEWARRAY, "java/lang/Object");
        for (int i = 0; i < f.args.size(); i++) {
            mv.visitInsn(DUP);
            pushInt(mv, i);
            emit(mv, cn, f.args.get(i));
            mv.visitInsn(AASTORE);
        }
        mv.visitMethodInsn(INVOKESTATIC, RUNTIME, "func",
                "(Ljava/lang/String;[Ljava/lang/Object;)" + OBJ, false);
    }

    /** Unloadable ClassLoader for the generated class, so it can be collected once the handle is released. */
    private static final class GeneratedClassLoader extends ClassLoader {
        GeneratedClassLoader(ClassLoader parent) {
            super(parent);
        }

        Class<?> define(String name, byte[] bytes) {
            return defineClass(name, bytes, 0, bytes.length);
        }
    }
}
