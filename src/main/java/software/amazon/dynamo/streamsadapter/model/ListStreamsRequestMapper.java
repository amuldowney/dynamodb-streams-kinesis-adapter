/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

/**
 * Container for the parameters to the ListStreams operation.
 */
public class ListStreamsRequestMapper {

    public static software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest convert(software.amazon.awssdk.services.kinesis.model.ListStreamsRequest internalRequest) {
        return software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest.builder()
                .exclusiveStartStreamArn(internalRequest.exclusiveStartStreamName())
                .limit(internalRequest.limit())
                .build();
    }
}