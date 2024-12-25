package com.github.gquintana.searchdump.configuration;

public class MissingConfigurationException extends RuntimeException {
    public MissingConfigurationException(String key) {
        super("Missing configuration " + key);
    }

}
