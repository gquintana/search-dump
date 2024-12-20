package com.github.gquintana.searchdump.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentReader;
import com.github.gquintana.searchdump.core.TechnicalException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class ElasticsearchDocumentReader implements SearchDocumentReader {
    private final ElasticsearchClient client;
    private final String scrollId;
    private final Time scrollTime;
    private int hitSize;
    private Iterator<Hit<Map>> hitIterator;

    public ElasticsearchDocumentReader(String index, int searchSize, String searchScrollTime, ElasticsearchClient client) {
        this.client = client;
        try {
            scrollTime = new Time.Builder().time(searchScrollTime).build();
            SearchRequest searchRequest = new SearchRequest.Builder()
                    .index(index)
                    .size(searchSize)
                    .scroll(scrollTime)
                    .build();
            SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
            this.scrollId = searchResponse.scrollId();
            setHits(searchResponse.hits());
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
    }

    private void setHits(HitsMetadata hits) {
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
            ScrollResponse<Map> scrollResponse = client.scroll(scrollRequest, Map.class);
            setHits(scrollResponse.hits());
        } catch (IOException e) {
            throw new TechnicalException(e);
        }
        return hitIterator.hasNext();
    }

    @Override
    public SearchDocument next() {
        Hit<Map> hit = hitIterator.next();
        return new SearchDocument(hit.index(), hit.id(), hit.source());
    }
}
