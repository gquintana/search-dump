package com.github.gquintana.searchdump.core;

public interface QuietCloseable extends AutoCloseable {
    @Override
    default void close() {}
}
