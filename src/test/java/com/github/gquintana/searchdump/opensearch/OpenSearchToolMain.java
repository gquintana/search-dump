package com.github.gquintana.searchdump.opensearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.github.gquintana.searchdump.Main;
import com.github.gquintana.searchdump.SearchHelper;

public class OpenSearchToolMain {
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
        try (OpenSearchWriter writer = new OpenSearchWriter(
                new OpenSearchClientFactory("https://localhost:9201", "admin", "admin", false),
                10, JsonMapper.builder().build())) {
            SearchHelper helper = new SearchHelper("test-1");
            helper.createAndFill(writer);
            writer.refreshIndex("test-1");
        }
    }

    private static void backup() {
        Main.main("--reader-type", "opensearch",
                "--reader-url", "https://localhost:9201",
                "--reader-username", "admin",
                "--reader-password", "admin",
                "--reader-ssl-verify", "false",
                "--writer-type", "zip",
                "--writer-file", "test.zip",
                "--writer-file-size", "10",
                "--index", "test-1"
        );
    }

    private static void restore() {
        Main.main(
                "--reader-type", "zip",
                "--reader-file", "test.zip",
                "--writer-type", "opensearch",
                "--writer-url", "https://localhost:9200",
                "--writer-username", "admin",
                "--writer-password", "admin",
                "--writer-ssl-verify", "false",
                "--index", "test-1"
        );
    }
}
