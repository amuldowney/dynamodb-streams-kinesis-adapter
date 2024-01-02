/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryRequest;

/**
 * Container for the parameters to the DescribeStream operation.
 */
public class DescribeStreamRequestMapper {
    public static software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest convert(DescribeStreamRequest internalRequest) {
        return software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest.builder()
            .exclusiveStartShardId(internalRequest.exclusiveStartShardId())
            .limit(internalRequest.limit())
            .streamArn(internalRequest.streamName())
            .build();
    }

    public static software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest convert(
        DescribeStreamSummaryRequest internalRequest) {
        return software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest.builder()
            .streamArn(internalRequest.streamName())
            .build();
    }
}