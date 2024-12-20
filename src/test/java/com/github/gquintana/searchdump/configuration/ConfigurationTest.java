package com.github.gquintana.searchdump.configuration;

import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationTest {
    @Test
    void properties() throws URISyntaxException {
        PropertiesConfiguration configuration = PropertiesConfiguration.load(Path.of(getClass().getResource("/test.properties").toURI()));
        assertEquals("Foo", configuration.getString("test.string").get());
        assertEquals(12, configuration.getInt("test.int").getAsInt());
        assertTrue(configuration.getInt("test.missing").isEmpty());
    }
    @Test
    void args() throws URISyntaxException {
        ArgsConfiguration configuration = new ArgsConfiguration("--test-string", "Foo", "--test-int", "12");
        assertEquals("Foo", configuration.getString("test.string").get());
        assertEquals(12, configuration.getInt("test.int").getAsInt());
        assertTrue(configuration.getInt("test.missing").isEmpty());
    }

    @Test
    void composite() throws URISyntaxException {
        CompositeConfiguration configuration = new CompositeConfiguration(
                new ArgsConfiguration("--test-string", "Bar", "--test-int", "12"),
                PropertiesConfiguration.load(Path.of(getClass().getResource("/test.properties").toURI()))
        );
        assertEquals("Bar", configuration.getString("test.string").get());
        assertEquals(12, configuration.getInt("test.int").getAsInt());
        assertTrue(configuration.getInt("test.missing").isEmpty());
    }
}