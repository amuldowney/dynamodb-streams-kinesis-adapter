///*
// * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// *
// * SPDX-License-Identifier: Apache-2.0
// */
//package software.amazon.dynamo.streamsadapter.model;
//
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.when;
//
//import org.junit.Before;
//import org.junit.Test;
//import org.mockito.Mock;
//import org.mockito.MockitoAnnotations;
//
//import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
//import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
//
//public class DescribeStreamResponseMapperTest {
//
//    @Mock
//    private DescribeStreamResponse mockResult;
//
//    @Mock
//    private StreamDescription mockDescription;
//
//    private software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse adapter;
//
//    @Before
//    public void setUpTest() {
//        MockitoAnnotations.initMocks(this);
//        when(mockResult.streamDescription()).thenReturn(mockDescription);
//    }
//
//    @Test(expected = UnsupportedOperationException.class)
//    public void testSetStreamDescription() {
//        adapter.setStreamDescription(null);
//    }
//
//    @Test(expected = UnsupportedOperationException.class)
//    public void testWithStreamDescription() {
//        adapter.withStreamDescription(null);
//    }
//
//}