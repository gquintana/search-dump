package com.github.gquintana.searchdump.core;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ListSplitter {
    public static <T> List<List<T>> split(List<T> list, int partitionCount) {
        if (list.size() <= partitionCount) {
            return list.stream().map(List::of).toList();
        }
        int partitionSize = list.size() / partitionCount + 1;
        List<List<T>> partitions = IntStream.range(0, partitionCount)
                .mapToObj(i -> (List<T>) new ArrayList<T>(partitionSize))
                .toList();
        for (int i = 0; i < list.size(); i++) {
            partitions.get(i % partitionCount).add(list.get(i));
        }
        return partitions;
    }
}
