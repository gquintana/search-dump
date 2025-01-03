package com.github.gquintana.searchdump.elasticsearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.Main;
import com.github.gquintana.searchdump.SearchHelper;

public class ElasticsearchToolMain {
    public static void main(String[] args) {
        switch (args[0]) {
            case "backup":
                backup();
                break;
            case "init":
                init();
                break;
            case "restore":
                restore();
                break;

        }

    }

    private static void init() {
        try (ElasticsearchWriter writer = new ElasticsearchWriter(
                new ElasticsearchClientFactory("http://localhost:9200", "elastic", "elastic", true),
                10, JsonMapper.builder().build())) {
            for (int i = 1; i < 3; i++) {
                SearchHelper helper = new SearchHelper("test-" + i);
                helper.createAndFill(writer);
                writer.refreshIndex("test-" + i);

            }
        }
    }

    private static void backup() {
        Main.main("--reader-type", "elasticsearch",
                "--reader-url", "http://localhost:9200",
                "--reader-username", "elastic",
                "--reader-password", "elastic",
                "--writer-type", "zip",
                "--writer-file", "test.zip",
                "--writer-file-size", "10",
                "--index", "test-*"
        );
    }

    private static void restore() {
        Main.main(
                "--reader-type", "zip",
                "--reader-file", "test.zip",
                "--writer-type", "elasticsearch",
                "--writer-url", "http://localhost:9200",
                "--writer-username", "elastic",
                "--writer-password", "elastic",
                "--writer-ssl-verify", "false",
                "--index", "test-*"
        );
    }
}
