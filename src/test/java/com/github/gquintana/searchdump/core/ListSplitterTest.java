package com.github.gquintana.searchdump.core;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListSplitterTest {
    @Test
    void normal() {
        List<Integer> input = IntStream.range(0, 10).boxed().toList();
        List<List<Integer>> output = ListSplitter.split(input, 4);
        assertEquals(4, output.size());
        for(List<Integer> split: output) {
            assertTrue(split.size() == 2 || split.size() == 3);
        }
    }

    @Test
    void notEnough() {
        List<Integer> input = IntStream.range(0, 3).boxed().toList();
        List<List<Integer>> output = ListSplitter.split(input, 4);
        assertEquals(3, output.size());
        for(List<Integer> split: output) {
            assertEquals(1, split.size());
        }
    }
}