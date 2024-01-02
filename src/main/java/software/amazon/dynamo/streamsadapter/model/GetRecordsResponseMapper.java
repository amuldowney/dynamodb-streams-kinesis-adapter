/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;

/**
 * Represents the output of a GetRecords operation.
 */
public class GetRecordsResponseMapper {
    public static software.amazon.awssdk.services.kinesis.model.GetRecordsResponse convert(GetRecordsResponse internalResult) {
        return convert(internalResult, true);
    }

    public static software.amazon.awssdk.services.kinesis.model.GetRecordsResponse convert(GetRecordsResponse internalResult, boolean generateRecordDataBytes) {
        return software.amazon.awssdk.services.kinesis.model.GetRecordsResponse.builder()
                .records(internalResult.records().stream().map(r -> RecordMapper.convert(r, generateRecordDataBytes)).collect(java.util.stream.Collectors.toList()))
                .nextShardIterator(internalResult.nextShardIterator())
                .build();
    }
}