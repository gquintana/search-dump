package com.github.gquintana.searchdump.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiGlobMatcherTest {

    @Test
    void match() {
        MultiGlobMatcher matcher = new MultiGlobMatcher(List.of("foo.json","ba*.json"));
        assertTrue(matcher.matches("foo.json"));
        assertTrue(matcher.matches("bar.json"));
        assertTrue(matcher.matches("baz.json"));
        assertFalse(matcher.matches("qix.json"));
    }

}