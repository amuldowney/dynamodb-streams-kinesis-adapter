/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

/**
 * Container for all information describing a single DynamoDB Stream.
 */
public class StreamDescriptionMapper {
    // Evaluate each StreamStatus.toString() only once
    private static final String STREAM_STATUS_DYNAMODB_DISABLED = software.amazon.awssdk.services.dynamodb.model.StreamStatus.DISABLED.toString();
    private static final String STREAM_STATUS_DYNAMODB_DISABLING = software.amazon.awssdk.services.dynamodb.model.StreamStatus.DISABLING.toString();
    private static final String STREAM_STATUS_DYNAMODB_ENABLED = software.amazon.awssdk.services.dynamodb.model.StreamStatus.ENABLED.toString();
    private static final String STREAM_STATUS_DYNAMODB_ENABLING = software.amazon.awssdk.services.dynamodb.model.StreamStatus.ENABLING.toString();
    private static final String STREAM_STATUS_KINESIS_ACTIVE = StreamStatus.ACTIVE.toString();
    private static final String STREAM_STATUS_KINESIS_CREATING = StreamStatus.CREATING.toString();

    public static software.amazon.awssdk.services.kinesis.model.StreamDescription convert(software.amazon.awssdk.services.dynamodb.model.StreamDescription internalDescription) {
        List<software.amazon.awssdk.services.kinesis.model.Shard> shards = new ArrayList<>();
        for (software.amazon.awssdk.services.dynamodb.model.Shard shard : internalDescription.shards()) {
            shards.add(ShardMapper.convert(shard));
        }

        return software.amazon.awssdk.services.kinesis.model.StreamDescription.builder()
            .streamName(internalDescription.streamArn())
            .streamARN(internalDescription.streamArn())
            .streamStatus(getStreamStatus(internalDescription.streamStatusAsString()))
            .shards(shards)
            .hasMoreShards(internalDescription.lastEvaluatedShardId() != null)
            .build();

    }

    /**
     * @return The current status of the stream being described.
     */
    public static String getStreamStatus(String status) {
        if (STREAM_STATUS_DYNAMODB_ENABLED.equals(status)) {
            status = STREAM_STATUS_KINESIS_ACTIVE;
        } else if (STREAM_STATUS_DYNAMODB_ENABLING.equals(status)) {
            status = STREAM_STATUS_KINESIS_CREATING;
        } else if (STREAM_STATUS_DYNAMODB_DISABLED.equals(status)) {
            // streams are valid for 24hrs after disabling and
            // will continue to support read operations
            status = STREAM_STATUS_KINESIS_ACTIVE;
        } else if (STREAM_STATUS_DYNAMODB_DISABLING.equals(status)) {
            status = STREAM_STATUS_KINESIS_ACTIVE;
        } else {
            throw new UnsupportedOperationException("Unsupported StreamStatus: " + status);
        }
        return status;
    }
}