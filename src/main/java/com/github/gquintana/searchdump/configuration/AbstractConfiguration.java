package com.github.gquintana.searchdump.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public abstract class AbstractConfiguration implements Configuration {

    @Override
    public Optional<String> getString(String key) {
        return Optional.ofNullable(getProperty(key)).map(String::trim).filter(s -> !s.isEmpty());
    }

    protected abstract String getProperty(String key);

    @Override
    public OptionalInt getInt(String key) {
        Optional<Integer> i = getString(key).map(s -> {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Not an integer " + key + ":" + s);
            }
        });
        return i.isPresent() ? OptionalInt.of(i.get()) : OptionalInt.empty();
    }

    @Override
    public Optional<Boolean> getBoolean(String key) {
        return getString(key).map(Boolean::parseBoolean);
    }

    public List<String> getStrings(String key) {
        return getString(key)
                .map(s -> List.of(s.split(",")))
                .orElse(Collections.emptyList());
    }
}
