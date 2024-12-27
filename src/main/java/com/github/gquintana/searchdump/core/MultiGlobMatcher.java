package com.github.gquintana.searchdump.core;

import java.util.List;
import java.util.stream.Collectors;

public class MultiGlobMatcher {
    private final List<GlobMatcher> globs;

    public MultiGlobMatcher(List<String> globs) {
        this.globs = globs.stream().map(GlobMatcher::new).collect(Collectors.toList());
    }

    public boolean matches(String s) {
        return this.globs.stream().anyMatch(glob -> glob.matches(s));
    }
}