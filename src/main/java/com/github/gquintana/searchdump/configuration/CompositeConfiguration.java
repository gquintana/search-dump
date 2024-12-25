package com.github.gquintana.searchdump.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

public class CompositeConfiguration implements Configuration{
    private final List<Configuration> configurations;
    public CompositeConfiguration(List<Configuration> configurations) {
        this.configurations = configurations;
    }
    public CompositeConfiguration(Configuration ... configurations) {
        this(List.of(configurations));
    }

    @FunctionalInterface
    private interface ConfigurationGetter<T> {
        Optional<T> get(Configuration configuration);
    }

    private <T> Optional<T> getFirst(ConfigurationGetter<T> getter) {
        return configurations.stream()
                .flatMap(c -> getter.get(c).stream())
                .findFirst();
    }

    @Override
    public Optional<String> getString(String key) {
        return getFirst(c -> c.getString(key));
    }

    @Override
    public List<String> getStrings(String key) {
        return getFirst(c -> Optional.ofNullable(c.getStrings(key)))
                .orElse(Collections.emptyList());
    }

    @Override
    public OptionalInt getInt(String key) {
        return configurations.stream()
                .map(c -> c.getInt(key))
                .filter(OptionalInt::isPresent)
                .mapToInt(OptionalInt::getAsInt)
                .findFirst();
    }
    @Override
    public Optional<Boolean> getBoolean(String key) {
        return getFirst(c -> c.getBoolean(key));
    }


}
