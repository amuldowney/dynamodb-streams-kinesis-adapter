/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

public class GetRecordsResponseMapperTest {
    private final String TEST_STRING = "TestString";

    @Test
    public void testRealDataNoRecords() {
        GetRecordsResponse result = createResult(false);
        software.amazon.awssdk.services.kinesis.model.GetRecordsResponse resultAdapter = GetRecordsResponseMapper.convert(result);
        assertEquals(result.nextShardIterator(), resultAdapter.nextShardIterator());
        assertEquals(result.records().size(), resultAdapter.records().size());
    }

    @Test
    public void testRealDataWithRecords() {
        GetRecordsResponse result = createResult(true);
        software.amazon.awssdk.services.kinesis.model.GetRecordsResponse resultAdapter = GetRecordsResponseMapper.convert(result);
        assertEquals(result.nextShardIterator(), resultAdapter.nextShardIterator());
        assertEquals(result.records().size(), resultAdapter.records().size());
    }

    private GetRecordsResponse createResult(Boolean withRecord) {
        List<Record> records = new java.util.ArrayList<Record>();
        if (withRecord) {
            records.add(Record.builder().dynamodb(StreamRecord.builder().sequenceNumber("sequenceNumber").build()).build());
        }
        return GetRecordsResponse.builder().records(records).nextShardIterator(TEST_STRING).build();
    }
}