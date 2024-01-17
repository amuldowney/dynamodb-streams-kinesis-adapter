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
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

public class GetShardIteratorRequestAdapterTest {
    private final String TEST_STRING = "TestString";

    @Test
    public void testRealData() {
        GetShardIteratorRequest request = createRequest();
        software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest requestAdapter = GetShardIteratorRequestMapper.convert(request);
        assertEquals(request.startingSequenceNumber(), requestAdapter.sequenceNumber());
        assertEquals(request.shardId(), requestAdapter.shardId());
        assertEquals(request.shardIteratorTypeAsString(), requestAdapter.shardIteratorTypeAsString());
        assertEquals(request.streamName(), requestAdapter.streamArn());
    }

    private GetShardIteratorRequest createRequest() {
        return GetShardIteratorRequest.builder()
            .shardId(TEST_STRING)
            .startingSequenceNumber(TEST_STRING)
            .shardIteratorType(ShardIteratorType.LATEST)
            .streamName(TEST_STRING)
            .build();
    }

}