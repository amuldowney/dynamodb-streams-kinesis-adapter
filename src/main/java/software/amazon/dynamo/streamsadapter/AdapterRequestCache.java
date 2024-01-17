/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;

import software.amazon.awssdk.awscore.AwsRequest;

/**
 * Cache for mapping Kinesis requests to DynamoDB Streams requests. Evicts oldest cache entry if adding a new entry exceeds the cache capacity.
 */
public class AdapterRequestCache {
    /**
     * Map for request lookup.
     */
    private final HashMap<Integer, AwsRequest> cacheMap = new HashMap<>();
    /**
     * Deque for evicting old cache entries.
     */
    private final Deque<Integer> evictQueue = new LinkedList<Integer>();
    /**
     * Capacity for the cache.
     */
    private final int capacity;

    public AdapterRequestCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be a positive number");
        }
        this.capacity = capacity;
    }

    /**
     * Adds an entry to the cache.
     *
     * @param request        Kinesis request
     * @param requestAdapter DynamoDB adapter client wrapper for the Kinesis request
     */
    public synchronized void addEntry(AwsRequest request, AwsRequest requestAdapter) {
        if (null == request || null == requestAdapter) {
            throw new IllegalArgumentException("Request and adapter request must not be null");
        }
        if (evictQueue.size() == capacity) {
            Integer evicted = evictQueue.removeLast();
            cacheMap.remove(evicted);
        }
        evictQueue.addFirst(System.identityHashCode(request));
        cacheMap.put(System.identityHashCode(request), requestAdapter);
    }

    /**
     * Gets the actual DynamoDB Streams request made for a Kinesis request.
     *
     * @param request Kinesis request
     * @return actual DynamoDB Streams request made for the associated Kinesis request
     */
    public synchronized AwsRequest getEntry(AwsRequest request) {//todo find uses for this
        if (null == request) {
            throw new IllegalArgumentException("Request must not be null");
        }
        return cacheMap.get(System.identityHashCode(request));
    }

}