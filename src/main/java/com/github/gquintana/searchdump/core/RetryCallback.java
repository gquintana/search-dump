package com.github.gquintana.searchdump.core;

import java.io.IOException;

@FunctionalInterface
public interface RetryCallback<R> {
    R apply() throws IOException;
}
