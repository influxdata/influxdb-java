package org.influxdb.impl;

public enum StringUtil {
    INSTANCE;

    public static boolean isNullOrEmpty(final String str) {
        if (str != null && !str.isEmpty()) {
            return false;
        }
        return true;
    }
}
