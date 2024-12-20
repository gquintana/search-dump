package com.github.gquintana.searchdump.configuration;

import java.util.Optional;
import java.util.OptionalInt;

public interface Configuration {
    Optional<String> getString(String key);
    OptionalInt getInt(String key);
    Optional<Boolean> getBoolean(String key);
}
