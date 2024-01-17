/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;

public class ShardMapperTest {
    private final String TEST_STRING = "TestString";

    @Test
    public void testRealData() {
        Shard shard = createShard();
        software.amazon.awssdk.services.kinesis.model.Shard shardMapper = ShardMapper.convert(shard);
        assertEquals(shard.shardId(), shardMapper.shardId());
        assertEquals(shard.parentShardId(), shardMapper.parentShardId());
        assertEquals(shard.sequenceNumberRange().startingSequenceNumber(), shardMapper.sequenceNumberRange().startingSequenceNumber());
        assertEquals(shard.sequenceNumberRange().endingSequenceNumber(), shardMapper.sequenceNumberRange().endingSequenceNumber());
    }

    private Shard createShard() {
        return Shard.builder().shardId(TEST_STRING).parentShardId(TEST_STRING)
            .sequenceNumberRange(SequenceNumberRange.builder().startingSequenceNumber(TEST_STRING).endingSequenceNumber(TEST_STRING).build()).build();
    }

}