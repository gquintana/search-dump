package com.github.gquintana.searchdump.opensearch;

import com.github.gquintana.searchdump.core.SearchDocument;
import com.github.gquintana.searchdump.core.SearchDocumentReader;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Time;
import org.opensearch.client.opensearch.core.*;
import org.opensearch.client.opensearch.core.search.Hit;
import org.opensearch.client.opensearch.core.search.HitsMetadata;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class OpenSearchDocumentReader implements SearchDocumentReader {
    private final OpenSearchClient client;
    private final String scrollId;
    private final Time scrollTime;
    private int hitSize;
    private Iterator<Hit<Map>> hitIterator;

    public OpenSearchDocumentReader(String index, int searchSize, String searchScrollTime, OpenSearchClient client) {
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
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
        }
        return hitIterator.hasNext();
    }

    @Override
    public SearchDocument next() {
        Hit<Map> hit = hitIterator.next();
        return new SearchDocument(hit.index(), hit.id(), hit.source());
    }
}
