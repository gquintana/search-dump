package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.CreateOperation;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentWriter;
import com.github.gquintana.searchdump.core.TechnicalException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchDocumentWriter implements SearchDocumentWriter {
    private final ElasticsearchClient client;
    private final String index;
    private final int bulkSize;
    private final List<BulkOperation> bulkOperations;

    public ElasticsearchDocumentWriter(ElasticsearchClient client, String index, int bulkSize) {
        this.client = client;
        this.index = index;
        this.bulkSize = bulkSize;
        this.bulkOperations = new ArrayList<>(bulkSize);
    }

    @Override
    public void write(SearchDocument document) {
        CreateOperation<Map<String, Object>> createOperation = new CreateOperation.Builder<Map<String, Object>>()
                .document(document.source())
                .index(this.index)
                .id(document.id())
                .build();
        bulkOperations.add(new BulkOperation.Builder().create(createOperation).build());
        if (bulkOperations.size() >= bulkSize) {
            flush();
        }
    }

    @Override
    public void flush()  {
        if (bulkOperations.isEmpty()) {
            return;
        }
        try {
            client.bulk(new BulkRequest.Builder().index(this.index).operations(bulkOperations).build());
            this.bulkOperations.clear();
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public void close() {
        flush();
    }
}
