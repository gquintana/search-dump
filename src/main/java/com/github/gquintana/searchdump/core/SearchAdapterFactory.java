package com.github.gquintana.searchdump.core;

import com.github.gquintana.searchdump.configuration.Configuration;

public interface SearchAdapterFactory {
    SearchReader createReader(Configuration configuration);
    SearchWriter createWriter(Configuration configuration);
}
