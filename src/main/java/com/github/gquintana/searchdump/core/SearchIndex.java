package com.github.gquintana.searchdump.core;

import java.util.Map;

public record SearchIndex(
        String name,
        Map<String, Object> settings,
        Map<String, Object> mappings,
        Map<String, Object> aliases) {

}
