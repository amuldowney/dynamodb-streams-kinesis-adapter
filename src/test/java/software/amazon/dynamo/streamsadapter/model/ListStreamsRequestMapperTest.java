/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import software.amazon.awssdk.services.kinesis.model.ListStreamsRequest;

public class ListStreamsRequestMapperTest {
    private final String TEST_STRING = "TestString";

    private final Integer TEST_INT = 42;

    @Test
    public void testRealData() {
        ListStreamsRequest request = createRequest();
        software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest requestAdapter = ListStreamsRequestMapper.convert(request);
        assertEquals(request.exclusiveStartStreamName(), requestAdapter.exclusiveStartStreamArn());
        assertEquals(request.limit(), requestAdapter.limit());
    }

    private ListStreamsRequest createRequest() {
        return ListStreamsRequest.builder().exclusiveStartStreamName(TEST_STRING).limit(TEST_INT).build();
    }

}