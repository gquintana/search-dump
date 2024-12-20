package com.github.gquintana.searchdump.opensearch;

import com.github.gquintana.searchdump.Main;

public class ZipFileOpensearchMain {
    public static void main(String[] args) {
        Main.main(
                "--reader-type", "zip",
                "--reader-file", "test.zip",
                "--writer-type", "opensearch",
                "--writer-url","https://localhost:9201",
                "--writer-username", "admin",
                "--writer-password", "admin",
                "--writer-ssl-verify", "false",
                "--index", "test-1"
                );
    }
}
