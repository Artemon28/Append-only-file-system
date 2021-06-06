package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.DatabaseCache;

import java.util.LinkedHashMap;
import java.util.Map;


public class DatabaseCacheImpl implements DatabaseCache {
    private static final int CAPACITY = 5_000;


    private static final int databaseSize = 1000;
    private final Map<String, byte[]> cache;

    public DatabaseCacheImpl() {
        this.cache = new LinkedHashMap<>(databaseSize, 1f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
                return size() > databaseSize;
            }
        };
    }

    @Override
    public byte[] get(String key) {
        return cache.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cache.put(key, value);
    }

    @Override
    public void delete(String key) {
        cache.remove(key);
    }
}
