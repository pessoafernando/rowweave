package io.rowweave.mem;

/**
 * Explicit failure raised when the memory budget is exhausted and a spill still leaves too little room, instead of letting the process die with an OOM.
 */
public final class MemoryBudgetExceededException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MemoryBudgetExceededException(String message) {
        super(message);
    }
}
