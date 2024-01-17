/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;

public class DescribeStreamRequestMapperTest {
    private final String TEST_STRING = "TestString";
    private final Integer TEST_INT = 42;

    @Test
    public void testRealData() {
        DescribeStreamRequest request = createRequest();
        software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest requestAdapter = DescribeStreamRequestMapper.convert(request);
        assertEquals(request.exclusiveStartShardId(), requestAdapter.exclusiveStartShardId());
        assertEquals(request.limit(), requestAdapter.limit());
        assertEquals(request.streamName(), requestAdapter.streamArn());
    }

    private DescribeStreamRequest createRequest() {
        return DescribeStreamRequest.builder().exclusiveStartShardId(TEST_STRING).limit(TEST_INT).streamName(TEST_STRING).build();
    }

}