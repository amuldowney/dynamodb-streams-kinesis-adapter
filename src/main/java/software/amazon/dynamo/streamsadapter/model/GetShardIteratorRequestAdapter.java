/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;

/**
 * Container for the parameters to the GetShardIterator operation.
 */
public class GetShardIteratorRequestAdapter {
    // Evaluate each ShardIteratorType toString() only once.
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_AT_SEQUENCE_NUMBER = ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_AFTER_SEQUENCE_NUMBER = ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_LATEST = ShardIteratorType.LATEST.toString();
    private static final String SHARD_ITERATOR_TYPE_DYNAMODB_TRIM_HORIZON = ShardIteratorType.TRIM_HORIZON.toString();

    private static final String SHARD_ITERATOR_TYPE_KINESIS_AFTER_SEQUENCE_NUMBER = com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_KINESIS_AT_SEQUENCE_NUMBER = com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER.toString();
    private static final String SHARD_ITERATOR_TYPE_KINESIS_LATEST = com.amazonaws.services.kinesis.model.ShardIteratorType.LATEST.toString();
    private static final String SHARD_ITERATOR_TYPE_KINESIS_TRIM_HORIZON = com.amazonaws.services.kinesis.model.ShardIteratorType.TRIM_HORIZON.toString();

    public static software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest convert(software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest internalRequest) {
        return software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.builder()
                .streamArn(internalRequest.streamName())
                .shardId(internalRequest.shardId())
                .sequenceNumber(internalRequest.startingSequenceNumber())
                .shardIteratorType(internalRequest.shardIteratorTypeAsString())
                .build();
    }
    /**
     * @param shardIteratorType Determines how the shard iterator is used to start reading data
     *                          records from the shard.
     */
    //TODO does this matter?
    //public void setShardIteratorType(String shardIteratorType) {
    //    if (SHARD_ITERATOR_TYPE_DYNAMODB_TRIM_HORIZON.equals(shardIteratorType)) {
    //        internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_TRIM_HORIZON);
    //    } else if (SHARD_ITERATOR_TYPE_DYNAMODB_LATEST.equals(shardIteratorType)) {
    //        internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_LATEST);
    //    } else if (SHARD_ITERATOR_TYPE_DYNAMODB_AT_SEQUENCE_NUMBER.equals(shardIteratorType)) {
    //        internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_AT_SEQUENCE_NUMBER);
    //    } else if (SHARD_ITERATOR_TYPE_DYNAMODB_AFTER_SEQUENCE_NUMBER.equals(shardIteratorType)) {
    //        internalRequest.setShardIteratorType(SHARD_ITERATOR_TYPE_KINESIS_AFTER_SEQUENCE_NUMBER);
    //    } else {
    //        throw new IllegalArgumentException("Unsupported ShardIteratorType: " + shardIteratorType);
    //    }
    //}
}