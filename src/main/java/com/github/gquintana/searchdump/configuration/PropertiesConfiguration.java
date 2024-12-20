package com.github.gquintana.searchdump.configuration;

import com.github.gquintana.searchdump.core.TechnicalException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class PropertiesConfiguration extends AbstractConfiguration {
    private final Properties properties;

    public PropertiesConfiguration(Properties properties) {
        this.properties = properties;
    }

    public static PropertiesConfiguration load(Path path) {
        try (InputStream inputStream = Files.newInputStream(path)) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return new PropertiesConfiguration(properties);
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    protected String getProperty(String key) {
        return properties.getProperty(key);
    }
}

