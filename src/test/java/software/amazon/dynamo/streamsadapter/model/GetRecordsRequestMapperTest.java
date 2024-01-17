/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;


import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;

public class GetRecordsRequestMapperTest {
    private final String TEST_STRING = "TestString";
    private final Integer TEST_INT = 42;

    @Test
    public void testRealData() {
        GetRecordsRequest request = createRequest();
        software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest requestAdapter = GetRecordsRequestMapper.convert(request);
        assertEquals(request.shardIterator(), requestAdapter.shardIterator());
        assertEquals(request.limit(), requestAdapter.limit());
    }

    private GetRecordsRequest createRequest() {
        return GetRecordsRequest.builder().limit(TEST_INT).shardIterator(TEST_STRING).build();
    }

}