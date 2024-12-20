package com.github.gquintana.searchdump.configuration;

import java.util.HashMap;
import java.util.Map;

public class ArgsConfiguration extends AbstractConfiguration {
    private final Map<String, String> argMap = new HashMap<>();

    public ArgsConfiguration(String ... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid number of arguments");
        }
        for (int i = 0; i < args.length; i+=2) {
            if (args[i].startsWith("--")) {
                argMap.put(args[i].substring(2), args[i+1]);
            } else {
                throw new IllegalArgumentException("Invalid argument: " + args[i]);
            }
        }
    }

    @Override
    protected String getProperty(String key) {
        return argMap.get(key.replaceAll("\\.", "-").toLowerCase());
    }
}
