package io.rowweave.upgrade;

/** Admission projection used while existing work drains under its old authority. */
public final class UpgradeWriteAdmission {
    public enum Action {
        NEW_SUBMISSION,
        NEW_CLAIM,
        NEW_ASSIGNMENT,
        RECONCILIATION
    }

    private UpgradeWriteAdmission() { }

    public static boolean allowed(
            UpgradeAuthorityRecord authority, Action action) {
        if (authority == null || action == null) {
            throw new IllegalArgumentException("authority and action are required");
        }
        return action == Action.RECONCILIATION || authority.writesAllowed();
    }

    public static void requireAllowed(
            UpgradeAuthorityRecord authority, Action action) {
        if (!allowed(authority, action)) {
            throw new IllegalStateException("STORE_UPGRADE_ADMISSION_FROZEN: "
                    + action + " phase=" + authority.phase());
        }
    }
}
