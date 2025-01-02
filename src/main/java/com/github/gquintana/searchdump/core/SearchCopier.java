package com.github.gquintana.searchdump.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SearchCopier {
    private static final Logger LOGGER = LoggerFactory.getLogger(SearchCopier.class);
    private final SearchReader reader;
    private final SearchWriter writer;
    private final boolean skipFailed;
    private final boolean skipExisting;

    public SearchCopier(SearchReader reader, SearchWriter writer, boolean skipFailed, boolean skipExisting) {
        this.reader = reader;
        this.writer = writer;
        this.skipFailed = skipFailed;
        this.skipExisting = skipExisting;
    }

    public void copy(List<String> indices) {
        for(String index : reader.listIndices(indices)) {
            copy(index);
        }
    }

    public void copy(String index) {
        LOGGER.info("Preparing index {}", index);
        try {
            boolean alreadyExists = !copyIndex(index);
            if (alreadyExists && skipExisting) {
                LOGGER.warn("Index {} already exists: skipping", index);
                return;
            }
            copyDocuments(index);
        } catch (RuntimeException e) {
            LOGGER.warn("Index {} copy failed: {}", index, e.getMessage());
            if (!skipFailed) {
                throw e;
            }
        }
    }

    /**
     * @return true if new index was created
     */
    private boolean copyIndex(String index) {
        boolean created = writer.createIndex(reader.getIndex(index));
        if (created) {
            LOGGER.info("Created index {}", index);
        }
        return created;
    }

    private record WriteReport(long timestamp, long documentCount) {
        private WriteReport logIfNeeded(String index, long currentCount) {
            long currentTimestamp = System.currentTimeMillis();
            long deltaTimestamp = currentTimestamp - this.timestamp();
            if (deltaTimestamp > 60000L) {
                long docRate = (currentCount - documentCount)*1000/deltaTimestamp;
                LOGGER.info("Wrote {} documents to index {}, {} docs/s", currentCount, index, docRate);
                return new WriteReport(currentTimestamp, currentCount);
            } else {
                return this;
            }
        }
    }
    private void copyDocuments(String index) {
        long documentCount = 0;
        WriteReport report = new WriteReport(System.currentTimeMillis(), 0);
        try(SearchDocumentReader documentReader = reader.readDocuments(index);
            SearchDocumentWriter documentWriter = writer.writeDocuments(index)
        ) {
            while (documentReader.hasNext()) {
                documentWriter.write(documentReader.next());
                documentCount++;
                report = report.logIfNeeded(index, documentCount);
            }
            documentWriter.flush();
            LOGGER.info("Wrote {} documents to index {}", documentCount, index);
        }
    }
}
