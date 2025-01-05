package com.github.gquintana.searchdump.core;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetrierTest {
    private static class RetryException extends RuntimeException {
    }

    private final Retrier retrier = new Retrier(5, 100);


    @Test
    void noRetry() throws IOException {
        long start = System.currentTimeMillis();
        Boolean result = retrier.retry(() -> true);
        long duration = System.currentTimeMillis() - start;
        assertTrue(result);
        assertTrue(duration < 100);
    }

    @Test
    void twoRetries() throws IOException {
        long start = System.currentTimeMillis();
        AtomicInteger retryCount = new AtomicInteger();
        Boolean result = retrier.retry(() -> {
            if (retryCount.getAndIncrement() < 2) {
                throw new RetryException();
            }
            return true;
        });
        long duration = System.currentTimeMillis() - start;
        assertTrue(result);
        assertEquals(3, retryCount.get());
        assertTrue(duration > 100);
        assertTrue(duration < 300);
    }

    @Test
    void allRetries() {
        long start = System.currentTimeMillis();
        AtomicInteger retryCount = new AtomicInteger();
        assertThrowsExactly(RetryException.class, () -> retrier.retry(() -> {
            retryCount.getAndIncrement();
            throw new RetryException();
        }));
        long duration = System.currentTimeMillis() - start;
        assertEquals(5, retryCount.get());
        assertTrue(duration >= 500);
    }
}