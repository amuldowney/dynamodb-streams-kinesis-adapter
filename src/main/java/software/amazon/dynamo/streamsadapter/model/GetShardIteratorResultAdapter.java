/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;


/**
 * Represents the output of a GetShardIterator operation.
 */
public class GetShardIteratorResultAdapter  {
    public static software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse convert(software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse internalResult) {
        return software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse.builder()
                .shardIterator(internalResult.shardIterator())
                .build();
    }
}