/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import static software.amazon.dynamo.streamsadapter.util.TestUtil.createDummyDBShard;

import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;

public class StreamDescriptionMapperTest {
    private final String TEST_STRING = "TestString";

    @Test
    public void testRealDataNoShards() {
        StreamDescription stream = createStreamDescription(false);
        software.amazon.awssdk.services.kinesis.model.StreamDescription streamAdapter = StreamDescriptionMapper.convert(stream);
        assertEquals(stream.streamArn(), streamAdapter.streamName());
        assertEquals(stream.streamArn(), streamAdapter.streamARN());
        assertEquals(stream.shards().size(), streamAdapter.shards().size());
    }

    @Test
    public void testRealDataWithShards() {
        StreamDescription stream = createStreamDescription(true);
        software.amazon.awssdk.services.kinesis.model.StreamDescription streamAdapter = StreamDescriptionMapper.convert(stream);
        assertEquals(stream.streamArn(), streamAdapter.streamName());
        assertEquals(stream.streamArn(), streamAdapter.streamARN());
        assertEquals(stream.shards().size(), streamAdapter.shards().size());
    }

    private StreamDescription createStreamDescription(Boolean withShards) {
        java.util.List<Shard> shards = new java.util.ArrayList<Shard>();
        if (withShards) {
            shards.add(createDummyDBShard());
        }
        return StreamDescription.builder().streamStatus(StreamStatus.ENABLED).streamArn(TEST_STRING).shards(shards).build();
    }

}