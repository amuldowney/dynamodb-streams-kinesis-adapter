/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import java.math.BigInteger;

/**
 * A uniquely identified group of data records in a DynamoDB
 * stream.
 */
public class ShardMapper {

    static final BigInteger MIN_HASH_KEY = BigInteger.ZERO;
    static final BigInteger MAX_HASH_KEY = new BigInteger("2").pow(128).subtract(BigInteger.ONE);

    public static software.amazon.awssdk.services.kinesis.model.Shard convert(software.amazon.awssdk.services.dynamodb.model.Shard internalShard) {
        return  software.amazon.awssdk.services.kinesis.model.Shard.builder()
            .shardId(internalShard.shardId())
            .parentShardId(internalShard.parentShardId())
            .adjacentParentShardId(null)
            .hashKeyRange(software.amazon.awssdk.services.kinesis.model.HashKeyRange.builder()
                .startingHashKey(MIN_HASH_KEY.toString())
                .endingHashKey(MAX_HASH_KEY.toString())
                .build())
            .sequenceNumberRange(software.amazon.awssdk.services.kinesis.model.SequenceNumberRange.builder()
                .startingSequenceNumber(internalShard.sequenceNumberRange().startingSequenceNumber())
                .endingSequenceNumber(internalShard.sequenceNumberRange().endingSequenceNumber())
                .build())
            .build();
    }
}