/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import java.util.stream.Collectors;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import software.amazon.awssdk.services.kinesis.model.ListStreamsResponse;

/**
 * Represents the output of a ListStreams operation.
 */
public class ListStreamsResponseMapper {

    public static ListStreamsResponse convert(
        software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse internalResult) {
        return ListStreamsResponse.builder()
            .streamNames(internalResult.streams().stream().map(Stream::streamArn).collect(Collectors.toList()))
            .hasMoreStreams(internalResult.lastEvaluatedStreamArn() != null)
            .build();
    }
}