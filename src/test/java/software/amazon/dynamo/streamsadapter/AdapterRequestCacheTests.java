/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.dynamo.streamsadapter.model.GetRecordsRequestMapper;


public class AdapterRequestCacheTests {

    private static final int CACHE_SIZE = 50;

    @Test
    public void testSanityConstructor() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        assertTrue(requestCache instanceof AdapterRequestCache);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testZeroCapacity() {
        new AdapterRequestCache(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeCapacity() {
        Random r = new Random();
        int positiveNumber = r.nextInt(Integer.MAX_VALUE - 1) + 1;
        new AdapterRequestCache(-1 * positiveNumber);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRequestAdd() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        requestCache.addEntry(null, GetRecordsRequestMapper.convert(GetRecordsRequest.builder().build()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRequestAdapterAdd() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        requestCache.addEntry(GetRecordsRequest.builder().build(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullRequestGet() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        requestCache.getEntry(null);
    }

    @Test
    public void testCacheSanity() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        GetRecordsRequest request = GetRecordsRequest.builder().build();
      software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest requestAdapter = GetRecordsRequestMapper.convert(request);
        requestCache.addEntry(request, requestAdapter);
      AwsRequest entry = requestCache.getEntry(request);
        assertEquals(System.identityHashCode(requestAdapter), System.identityHashCode(entry));
    }

    @Test
    public void testEviction() {
        AdapterRequestCache requestCache = new AdapterRequestCache(CACHE_SIZE);
        int testLength = 2 * CACHE_SIZE;
        List<AwsRequest> requests = new ArrayList<AwsRequest>(testLength);
        List<AwsRequest> requestAdapters = new ArrayList<AwsRequest>(testLength);
        for (int i = 0; i < testLength; i++) {
            // Construct requests
          GetRecordsRequest request = GetRecordsRequest.builder().build();
          software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest requestAdapter = GetRecordsRequestMapper.convert(request);
            // Store references to request for validation
            requests.add(request);
            requestAdapters.add(requestAdapter);
            // Add entry to the request cache
            requestCache.addEntry(request, requestAdapter);

            // Verify request cache
            for (int j = 0; j <= i; j++) {
              AwsRequest expected = requestAdapters.get(j);
              AwsRequest actual = requestCache.getEntry(requests.get(j));
                if (j <= i - CACHE_SIZE) {
                    assertNull(actual);
                } else {
                    assertEquals(System.identityHashCode(expected), System.identityHashCode(actual));
                }
            }

        }
    }

}