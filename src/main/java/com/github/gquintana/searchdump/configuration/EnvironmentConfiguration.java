package com.github.gquintana.searchdump.configuration;

public class EnvironmentConfiguration extends AbstractConfiguration {
    @Override
    protected String getProperty(String key) {
        return System.getenv(key.toUpperCase().replace('.', '_').replace('-', '_'));
    }
}

