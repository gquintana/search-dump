package com.github.gquintana.searchdump.core;

public class GlobMatcher {
    private final String[] parts;
    public GlobMatcher(String glob) {
        parts = glob.split("\\*");
    }

    public boolean matches(String s) {
        int fromIndex = 0;
        if (parts.length == 0) {
            return true;
        }
        if (!parts[0].isEmpty()) {
        }
        for (int i = 0; i < parts.length; i++) {
            if (!parts[i].isEmpty()) {
                if (i == 0) {
                    if (s.startsWith(parts[0])) {
                        fromIndex = parts[0].length();
                    } else {
                        return false;
                    }
                } else {
                    int foundIndex = s.indexOf(parts[i], fromIndex);
                    if (foundIndex >= 0) {
                        fromIndex = foundIndex + parts[i].length();
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
