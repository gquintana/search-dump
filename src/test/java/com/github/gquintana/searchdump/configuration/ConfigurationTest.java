package com.github.gquintana.searchdump.configuration;

import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigurationTest {
    @Test
    void properties() throws URISyntaxException {
        PropertiesConfiguration configuration = PropertiesConfiguration.load(Path.of(getClass().getResource("/test.properties").toURI()));
        assertEquals("Foo", configuration.getString("test.string").get());
        assertEquals(List.of("Bar","Baz"), configuration.getStrings("test.strings"));
        assertEquals(12, configuration.getInt("test.int").getAsInt());
        assertTrue(configuration.getInt("test.missing").isEmpty());
    }
    @Test
    void args() throws URISyntaxException {
        ArgsConfiguration configuration = new ArgsConfiguration("--test-string", "Foo", "--test-int", "12", "--test-strings", "Bar,Baz");
        assertEquals("Foo", configuration.getString("test.string").get());
        assertEquals(List.of("Bar","Baz"), configuration.getStrings("test.strings"));
        assertEquals(12, configuration.getInt("test.int").getAsInt());
        assertTrue(configuration.getInt("test.missing").isEmpty());
    }

    @Test
    void composite() throws URISyntaxException {
        CompositeConfiguration configuration = new CompositeConfiguration(
                new ArgsConfiguration("--test-string", "Bar", "--test-int", "12", "--test-strings", "Baz,Qix"),
                PropertiesConfiguration.load(Path.of(getClass().getResource("/test.properties").toURI()))
        );
        assertEquals("Bar", configuration.getString("test.string").get());
        assertEquals(List.of("Baz","Qix"), configuration.getStrings("test.strings"));
        assertEquals(12, configuration.getInt("test.int").getAsInt());
        assertTrue(configuration.getInt("test.missing").isEmpty());
    }
}