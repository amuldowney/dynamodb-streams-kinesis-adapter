/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

/**
 * Container for the parameters to the GetShardIterator operation.
 */
public class GetShardIteratorRequestMapper {
    public static software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest convert(software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest internalRequest) {
        return software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest.builder()
                .streamArn(internalRequest.streamName())
                .shardId(internalRequest.shardId())
                .sequenceNumber(internalRequest.startingSequenceNumber())
                .shardIteratorType(internalRequest.shardIteratorTypeAsString())
                .build();
    }
}