package com.github.gquintana.searchdump.core;

import java.util.Map;

public record SearchDocument(String index,
                             String id,
                             Map<String, Object> source) {
}
