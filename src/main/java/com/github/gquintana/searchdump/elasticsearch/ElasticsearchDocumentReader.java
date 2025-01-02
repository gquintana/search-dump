package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.fasterxml.jackson.core.type.TypeReference;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentReader;
import com.github.gquintana.searchdump.core.TechnicalException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ElasticsearchDocumentReader implements SearchDocumentReader {
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {};
    private final ElasticsearchClient client;
    private final String scrollId;
    private final Time scrollTime;
    private int hitSize;
    private Iterator<Hit<Map<String, Object>>> hitIterator;

    public ElasticsearchDocumentReader(String index, int searchSize, String searchScrollTime, ElasticsearchClient client) {
        this.client = client;
        try {
            scrollTime = new Time.Builder().time(searchScrollTime).build();
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(index)
                    .size(searchSize)
                    .scroll(scrollTime)
                    .build();
            SearchResponse<Map<String, Object>> searchResponse = client.search(searchRequest, MAP_TYPE_REF.getType());
            this.scrollId = searchResponse.scrollId();
            setHits(searchResponse.hits());
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private void setHits(HitsMetadata<Map<String, Object>> hits) {
        this.hitSize = hits.hits().size();
        this.hitIterator = hits.hits().iterator();
    }

    @Override
    public void close() {
        try {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest.Builder().scrollId(scrollId).build();
            client.clearScroll(clearScrollRequest);
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    @Override
    public boolean hasNext() {
        if (hitIterator.hasNext()) {
            return true;
        }
        if (hitSize == 0) {
            return false;
        }
        try {
            ScrollRequest scrollRequest = new ScrollRequest.Builder().scrollId(scrollId).scroll(scrollTime).build();
            ScrollResponse<Map<String, Object>> scrollResponse = client.scroll(scrollRequest, MAP_TYPE_REF.getType());
            setHits(scrollResponse.hits());
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
        return hitIterator.hasNext();
    }

    @Override
    public SearchDocument next() {
        Hit<Map<String, Object>> hit = hitIterator.next();
        return new SearchDocument(hit.index(), hit.id(), hit.source());
    }
}
