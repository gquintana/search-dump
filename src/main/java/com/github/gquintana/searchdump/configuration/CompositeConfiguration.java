package com.github.gquintana.searchdump.configuration;

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

    @Override
    public Optional<String> getString(String key) {
        return configurations.stream()
                .flatMap(c -> c.getString(key).stream())
                .findFirst();
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
        return configurations.stream()
                .flatMap(c -> c.getBoolean(key).stream())
                .findFirst();
    }
}
