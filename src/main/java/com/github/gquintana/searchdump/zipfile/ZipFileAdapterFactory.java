package com.github.gquintana.searchdump.zipfile;

import com.github.gquintana.searchdump.configuration.Configuration;
import com.github.gquintana.searchdump.configuration.MissingConfigurationException;
import com.github.gquintana.searchdump.core.SearchAdapterFactory;

import java.io.File;


public class ZipFileAdapterFactory implements SearchAdapterFactory {
    @Override
    public ZipFileSearchReader createReader(Configuration configuration) {
        return ZipFileSearchReader.read(
                new File(configuration.getString("reader.file").orElseThrow(() -> new MissingConfigurationException("reader.file")))
        );
    }

    @Override
    public ZipFileSearchWriter createWriter(Configuration configuration) {
        return ZipFileSearchWriter.write(
                new File(configuration.getString("writer.file").orElseThrow(() -> new MissingConfigurationException("writer.file"))),
                configuration.getInt("writer.file-size").orElse(1000)
        );
    }
}
