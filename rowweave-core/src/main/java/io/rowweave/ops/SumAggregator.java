package io.rowweave.ops;

import io.rowweave.mem.Accumulator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;

/**
 * The BigDecimal sum aggregator.
 * It accumulates at full precision throughout (order-independent, so combine and merge are safe) and calls setScale(10, HALF_UP) exactly once, before output.
 * An empty set (nothing non-null ever joined) returns null.
 *
 * <p>Implements {@link Accumulator}: spilling serializes the unrounded sum (null encoded as zero bytes) and a merge adds at full precision,
 * which keeps the result order-independent and bit-identical on both sides of a spill.
 */
public final class SumAggregator implements Accumulator {

    private static final int OUTPUT_SCALE = 10;

    // long unscaled fast path. While all added values share one scale and fit precision<=18, accumulate
    // their unscaled longs with Math.addExact (~1-3ns). On scale mismatch / precision>18 / overflow, upgrade to
    // the BigDecimal accumulator and never revert. State semantics (round once at output, null = empty) unchanged.
    private boolean fast = false;        // true once the first eligible value initialized the long path
    private long unscaled;               // fast-path running sum (valid iff fast && !upgraded)
    private int fixedScale;              // the single scale the fast path is locked to
    private boolean upgraded = false;    // true once fallen back to BigDecimal (sticky)
    private BigDecimal sum;              // BigDecimal accumulator (the authoritative sum once upgraded, or never-fast)
    private boolean any = false;         // saw at least one non-null value (distinguishes 0 sum from empty)

    public void add(BigDecimal value) {
        if (value == null) {
            return;
        }
        any = true;
        if (!upgraded) {
            if (!fast && sum == null) {
                // first non-null value: try to start the fast path
                if (value.precision() <= 18) {
                    fast = true;
                    fixedScale = value.scale();
                    unscaled = value.unscaledValue().longValue();
                    return;
                }
                upgradeTo(value); // first value already too big -> BigDecimal from the start
                return;
            }
            if (fast && value.scale() == fixedScale && value.precision() <= 18) {
                try {
                    unscaled = Math.addExact(unscaled, value.unscaledValue().longValue());
                    return;
                } catch (ArithmeticException overflow) {
                    upgradeFromFast();
                    // fall through to BigDecimal add below
                }
            } else if (fast) {
                upgradeFromFast(); // scale mismatch or precision>18 -> leave the fast path
            }
        }
        // BigDecimal (upgraded) path
        sum = (sum == null) ? value : sum.add(value);
    }

    /** Materialize the fast-path long sum into the BigDecimal accumulator, then switch to slow mode (sticky). */
    private void upgradeFromFast() {
        sum = BigDecimal.valueOf(unscaled, fixedScale);
        fast = false;
        upgraded = true;
    }

    /** First value couldn't use the fast path: seed BigDecimal accumulator directly. */
    private void upgradeTo(BigDecimal first) {
        sum = first;
        upgraded = true;
    }

    /** Current sum as an exact (unrounded) BigDecimal, or null if no non-null value was added. */
    private BigDecimal currentSum() {
        if (!any) {
            return null;
        }
        if (fast) {
            return BigDecimal.valueOf(unscaled, fixedScale);
        }
        return sum; // upgraded / never-fast
    }

    public void merge(SumAggregator other) {
        if (other == null) {
            return;
        }
        BigDecimal o = other.currentSum();
        if (o == null) {
            return;
        }
        addExact(o);
    }

    /** Add an exact BigDecimal into this accumulator honoring the fast/slow state (used by merge/mergeState). */
    private void addExact(BigDecimal value) {
        add(value); // add() already routes through fast/slow with the same correctness guarantees
    }

    public BigDecimal result() {
        BigDecimal s = currentSum();
        if (s == null) {
            return null;
        }
        return s.setScale(OUTPUT_SCALE, RoundingMode.HALF_UP);
    }

    @Override
    public byte[] writeState() {
        // null sum -> empty bytes; otherwise the exact unrounded plain string (full precision)
        BigDecimal s = currentSum();
        if (s == null) {
            return new byte[0];
        }
        return s.toPlainString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void mergeState(byte[] state) {
        if (state == null || state.length == 0) {
            return;
        }
        BigDecimal v = new BigDecimal(new String(state, StandardCharsets.UTF_8));
        addExact(v);
    }
}
