package com.github.gquintana.searchdump.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GlobMatcherTest {
    @Test
    void specific() {
        GlobMatcher matcher = new GlobMatcher("bar.json");
        assertFalse(matcher.matches("foo.json"));
        assertTrue(matcher.matches("bar.json"));
        assertFalse(matcher.matches("foo.txt"));
    }

    @Test
    void any() {
        GlobMatcher matcher = new GlobMatcher("*");
        assertTrue(matcher.matches("foo.json"));
        assertTrue(matcher.matches("bar.json"));
        assertTrue(matcher.matches("foo.txt"));
    }

    @Test
    void suffix() {
        GlobMatcher matcher = new GlobMatcher("*.json");
        assertTrue(matcher.matches("foo.json"));
        assertTrue(matcher.matches("bar.json"));
        assertFalse(matcher.matches("foo.txt"));
    }

    @Test
    void prefix() {
        GlobMatcher matcher = new GlobMatcher("foo.*");
        assertTrue(matcher.matches("foo.json"));
        assertFalse(matcher.matches("bar.json"));
        assertTrue(matcher.matches("foo.txt"));
    }

    @Test
    void middle() {
        GlobMatcher matcher = new GlobMatcher("*a*");
        assertTrue(matcher.matches("bar.json"));
        assertTrue(matcher.matches("baz.json"));
        assertFalse(matcher.matches("foo.json"));
    }

    @Test
    void suffixAndPrefix() {
        GlobMatcher matcher = new GlobMatcher("ba*.json");
        assertTrue(matcher.matches("bar.json"));
        assertTrue(matcher.matches("baz.json"));
        assertFalse(matcher.matches("foo.json"));
    }
}