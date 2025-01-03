package com.github.gquintana.searchdump.core;

import com.github.gquintana.searchdump.configuration.Configuration;

public interface SearchFactory {
    SearchReader createReader(Configuration configuration);
    SearchWriter createWriter(Configuration configuration);
}
