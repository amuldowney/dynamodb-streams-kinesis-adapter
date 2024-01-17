/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;

public class GetShardIteratorResultAdapterTest {
    private final String TEST_STRING = "TestString";

    @Test
    public void testRealData() {
        GetShardIteratorResponse result = createResult();
        software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse resultAdapter = GetShardIteratorResultAdapter.convert(result);
        assertEquals(result.shardIterator(), resultAdapter.shardIterator());
    }

    private GetShardIteratorResponse createResult() {
        return GetShardIteratorResponse.builder().shardIterator(TEST_STRING).build();
    }
}