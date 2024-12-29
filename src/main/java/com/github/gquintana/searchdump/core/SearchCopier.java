package com.github.gquintana.searchdump.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SearchCopier {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchCopier.class);
    private final SearchReader reader;
    private final SearchWriter writer;

    public SearchCopier(SearchReader reader, SearchWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public void copy(List<String> indices) {
        for(String index : reader.listIndices(indices)) {
            copy(index);
        }
    }

    public void copy(String index) {
        LOGGER.info("Preparing index {}", index);
        writer.createIndex(reader.getIndex(index));
        long documentCount = 0;
        try(SearchDocumentReader documentReader = reader.readDocuments(index);
            SearchDocumentWriter documentWriter = writer.writeDocuments(index)
        ) {
            while (documentReader.hasNext()) {
                documentWriter.write(documentReader.next());
                documentCount++;
            }
            documentWriter.flush();
            LOGGER.info("Wrote {} documents to index {}", documentCount, index);
        }
    }
}
