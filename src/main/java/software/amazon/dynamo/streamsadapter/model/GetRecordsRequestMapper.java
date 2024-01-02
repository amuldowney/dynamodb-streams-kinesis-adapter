/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

/**
 * Container for the parameters to the GetRecords operation.
 */
public class GetRecordsRequestMapper {
    /* The maximum number of records to return in a single getRecords call. */
    public static final Integer GET_RECORDS_LIMIT = 1000;
    public static software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest convert(software.amazon.awssdk.services.kinesis.model.GetRecordsRequest internalRequest ) {
        return software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest.builder()
                .limit(internalRequest.limit() != null && internalRequest.limit() > GET_RECORDS_LIMIT ? GET_RECORDS_LIMIT : internalRequest.limit())
                .shardIterator(internalRequest.shardIterator())
                .build();
    }
}