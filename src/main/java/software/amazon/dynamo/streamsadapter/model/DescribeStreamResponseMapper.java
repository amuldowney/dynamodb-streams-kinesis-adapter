/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import software.amazon.awssdk.services.kinesis.model.StreamMode;
import software.amazon.awssdk.services.kinesis.model.StreamModeDetails;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamSummaryResponse;
import software.amazon.awssdk.services.kinesis.model.StreamDescriptionSummary;

/**
 * Represents the output of a DescribeStream operation.
 */
//todo rename to new matching class names
public class DescribeStreamResponseMapper {

  public static software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse convert(
      DescribeStreamResponse internalResult) {
    return software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse.builder()
        .streamDescription(StreamDescriptionMapper.convert(internalResult.streamDescription()))
        .build();
  }

  public static DescribeStreamSummaryResponse convertToSummary(
      DescribeStreamResponse internalResult) {

    return DescribeStreamSummaryResponse.builder()
        .streamDescriptionSummary(StreamDescriptionSummary.builder()
            .streamName(internalResult.streamDescription().streamArn())
            .streamARN(internalResult.streamDescription().streamArn())
            .streamStatus(StreamDescriptionMapper.getStreamStatus(internalResult.streamDescription().streamStatusAsString()))
            .streamCreationTimestamp(internalResult.streamDescription().creationRequestDateTime())
            .streamModeDetails(StreamModeDetails.builder().streamMode(StreamMode.PROVISIONED).build())//todo hardcoded
            .encryptionType("NONE")//todo hardcoded
            //.keyId() Used w/ encryption to ID the key used
            .retentionPeriodHours(24)
            .openShardCount(internalResult.streamDescription().shards().size())//todo just # shards
            .consumerCount(0)//todo hardcoded
            .build()
        )
        .build();
  }
}