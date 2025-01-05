package com.github.gquintana.searchdump.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Retrier {
    private static final Logger LOGGER = LoggerFactory.getLogger(Retrier.class);
    private final int retryMax;
    protected final long retryWait;

    public Retrier(int retryMax, long retryWait) {
        this.retryMax = retryMax;
        this.retryWait = retryWait;
    }

    public <R>  R retry(RetryCallback<R> callback) throws IOException {
        Exception retriableException = null;
        for(int i = 0; i < retryMax; i++) {
            try {
                return callback.apply();
            } catch (IOException|RuntimeException e) {
                if (isRetriableException(e)) {
                    LOGGER.warn("Operation failed with {}: {}. Will retry", e.getClass().getSimpleName(), e.getMessage());
                    if (retriableException == null) {
                        retriableException = e;
                    }
                    try {
                        Thread.sleep(getRetryWaitTime(i));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                } else {
                    throw e;
                }
            }
        }
        if(retriableException instanceof RuntimeException runtimeException) {
            throw runtimeException;
        } else if(retriableException instanceof IOException ioException) {
            throw ioException;
        } else {
            throw new IllegalStateException();
        }
    }

    private long getRetryWaitTime(int retry) {
        return retryWait;
    }

    protected boolean isRetriableException(Exception e) {
        return true;
    }
}
