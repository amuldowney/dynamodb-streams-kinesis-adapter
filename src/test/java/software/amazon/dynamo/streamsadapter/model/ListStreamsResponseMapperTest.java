/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter.model;

import static org.junit.Assert.assertEquals;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.Stream;

public class ListStreamsResponseMapperTest {
    private final String TEST_STRING = "TestString";

    @Test
    public void testRealDataNoIds() {
        ListStreamsResponse result = createResult(false);
        software.amazon.awssdk.services.kinesis.model.ListStreamsResponse resultAdapter = ListStreamsResponseMapper.convert(result);
        List<String> streamArns = extractStreamArns(result);
        assertEquals(streamArns, resultAdapter.streamNames());
    }

    @Test
    public void testRealDataWithIds() {
        ListStreamsResponse result = createResult(true);
        software.amazon.awssdk.services.kinesis.model.ListStreamsResponse resultAdapter = ListStreamsResponseMapper.convert(result);
        assertEquals(extractStreamArns(result), resultAdapter.streamNames());
    }

    private List<String> extractStreamArns(ListStreamsResponse result) {
        List<Stream> streams = result.streams();
        List<String> streamArns = new ArrayList<>(streams.size());
        for (Stream stream : streams) {
            streamArns.add(stream.streamArn());
        }
        return streamArns;
    }

    private ListStreamsResponse createResult(Boolean withArns) {
        java.util.List<Stream> streams = new java.util.ArrayList<>();
        if (withArns) {
            Stream stream = Stream.builder().streamArn(TEST_STRING).build();
            streams.add(stream);
        }
        return ListStreamsResponse.builder().streams(streams).lastEvaluatedStreamArn(TEST_STRING).build();
    }

}