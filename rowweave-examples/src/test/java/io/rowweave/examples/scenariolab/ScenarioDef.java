package io.rowweave.examples.scenariolab;

/**
 * A scenario definition: one end-to-end engine run that stands on its own.
 *
 * <ul>
 * <li>BODY: the ordinary case. The body builds the graph and registers expectations, and the
 * framework commits, reads the results back and compares them.</li>
 * <li>EXPECT_THROWS: the negative case. The action is expected to throw the stated exception,
 * whether during graph validation or at commit.</li>
 * </ul>
 */
public final class ScenarioDef {

    /** The scenario body for kind=BODY. */
    public interface Body {
        void run(ScenarioLab lab) throws Exception;
    }

    public enum Kind { BODY, EXPECT_THROWS }

    public final String id;
    public final String title;
    public final Kind kind;
    private final Body body;
    private final Class<? extends Exception> expectedException;
    private final String expectedMessageFragment;

    private ScenarioDef(String id, String title, Kind kind, Body body,
                        Class<? extends Exception> expectedException,
                        String expectedMessageFragment) {
        this.id = id;
        this.title = title;
        this.kind = kind;
        this.body = body;
        this.expectedException = expectedException;
        this.expectedMessageFragment = expectedMessageFragment;
    }

    /** An ordinary scenario: the body builds the graph and registers expectations, and the framework commits and compares. */
    public static ScenarioDef of(String id, String title, Body body) {
        return new ScenarioDef(id, title, Kind.BODY, body, null, null);
    }

    /** A negative scenario: the action must throw expectedException or a subclass, otherwise the scenario fails. */
    public static ScenarioDef throwsOf(String id, String title,
                                       Class<? extends Exception> expectedException,
                                       String expectedMessageFragment,
                                       ThrowingAction action) {
        if (expectedMessageFragment == null || expectedMessageFragment.isEmpty()) {
            throw new IllegalArgumentException("negative scenario requires an expected message fragment");
        }
        return new ScenarioDef(id, title, Kind.EXPECT_THROWS, lab -> action.run(),
                expectedException, expectedMessageFragment);
    }

    public Body body() {
        return body;
    }

    public Class<? extends Exception> expectedException() {
        return expectedException;
    }

    public String expectedMessageFragment() {
        return expectedMessageFragment;
    }

    /** An action that may throw. */
    @FunctionalInterface
    public interface ThrowingAction {
        void run() throws Exception;
    }
}
