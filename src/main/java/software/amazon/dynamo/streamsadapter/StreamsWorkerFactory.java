/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBMultiStreamLeaseSerializer;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.processor.SingleStreamTracker;
import software.amazon.kinesis.retrieval.RetrievalConfig;
import software.amazon.kinesis.retrieval.polling.SynchronousPrefetchingRetrievalFactory;

/**
 * The StreamsWorkerFactory uses the Kinesis Client Library's Worker
 * class to provide convenient constructors for ease-of-use.
 */
public class StreamsWorkerFactory {
  private static final Log LOG = LogFactory.getLog(StreamsWorkerFactory.class);

  /**
   * @param shardRecordProcessorFactory Used to get record processor instances for processing data from shards
   * @param config                 Kinesis Client Library configuration
   * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
   * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
   * @param cloudWatchClient         Metrics factory used to emit metrics
   * @param executorService            ExecutorService to use for processing records (support for multi-threaded
   *                               consumption)
   * @return An instance of KCL worker injected with DynamoDB Streams specific dependencies.
   */
  public static Scheduler createDynamoDbStreamsWorkerz(
      ShardRecordProcessorFactory shardRecordProcessorFactory,
      KinesisClientLibConfiguration config,
      AmazonDynamoDBStreamsAdapterClient streamsClient,
      DynamoDbAsyncClient dynamoDBClient,
      CloudWatchAsyncClient cloudWatchClient,
      ExecutorService executorService) {
    //todo UUID??

    DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);
    InitialPositionInStreamExtended initialPositionInStreamExtended = InitialPositionInStreamExtended.newInitialPosition(config.getInitialPositionInStream());

    ConfigsBuilder configsBuilder =
        new ConfigsBuilder(new SingleStreamTracker(config.getStreamName(), initialPositionInStreamExtended), config.getApplicationName(), streamsClient,
            dynamoDBClient, cloudWatchClient, UUID.randomUUID().toString(),
            shardRecordProcessorFactory);

    //ConfigsBuilder construction is garbage so we have to do our own overlay of parts of the KinesisClientLibConfiguration
    //  .withCallProcessRecordsEvenForEmptyRecordList(true).withIdleTimeBetweenReadsInMillis(IDLE_TIME_2S);

    LeaseManagementConfig lmc = configsBuilder.leaseManagementConfig();
    lmc = lmc.initialLeaseTableReadCapacity(config.getInitialLeaseTableReadCapacity());
    lmc = lmc.initialLeaseTableWriteCapacity(config.getInitialLeaseTableWriteCapacity());
    lmc = lmc.shardSyncIntervalMillis(config.getShardSyncIntervalMillis());
    lmc = lmc.customShardDetectorProvider(cfg -> dynamoDBStreamsProxy);

    DynamoDBStreamsLeaseManagementFactory dynamoDBStreamsLeaseManagementFactory =
        new DynamoDBStreamsLeaseManagementFactory(
            streamsClient,
            dynamoDBClient,
            lmc.tableName(),
            lmc.workerIdentifier(),
            lmc.executorService(),
            lmc.failoverTimeMillis(),
            lmc.epsilonMillis(),
            lmc.maxLeasesForWorker(),
            lmc.maxLeasesToStealAtOneTime(),
            lmc.maxLeaseRenewalThreads(),
            lmc.cleanupLeasesUponShardCompletion(),
            lmc.ignoreUnexpectedChildShards(),
            lmc.shardSyncIntervalMillis(),
            lmc.consistentReads(),
            lmc.listShardsBackoffTimeInMillis(),
            lmc.maxListShardsRetryAttempts(),
            lmc.maxCacheMissesBeforeReload(),
            lmc.listShardsCacheAllowedAgeInSeconds(),
            lmc.cacheMissWarningModulus(),
            lmc.initialLeaseTableReadCapacity(),
            lmc.initialLeaseTableWriteCapacity(),
            new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator()),
            lmc.tableCreatorCallback(),
            lmc.dynamoDbRequestTimeout(),
            lmc.billingMode(),
            lmc.tags(),
            configsBuilder.retrievalConfig().streamTracker().isMultiStream() ?
                new DynamoDBMultiStreamLeaseSerializer() :
                new DynamoDBLeaseSerializer(),
            lmc.customShardDetectorProvider(),
            configsBuilder.retrievalConfig().streamTracker().isMultiStream(),
            lmc.leaseCleanupConfig());
    lmc.leaseManagementFactory(dynamoDBStreamsLeaseManagementFactory);


    RetrievalConfig rc = configsBuilder.retrievalConfig();
    rc = rc.retrievalFactory(new SynchronousPrefetchingRetrievalFactory(
        rc.streamTracker().streamConfigList().get(0).streamIdentifier().streamName(),//always a single stream tracker for dynamodb streams
        streamsClient,
        new DynamoDBStreamsRecordsFetcherFactory(),
        1000,
        executorService,
        300,
        Duration.ofSeconds(30)
    ));

    return new Scheduler(
        configsBuilder.checkpointConfig(),
        configsBuilder.coordinatorConfig(),
        lmc,
        configsBuilder.lifecycleConfig(),
        configsBuilder.metricsConfig(),
        configsBuilder.processorConfig(),
        rc
    );
  }

  private static DynamoDBStreamsProxy getDynamoDBStreamsProxy(KinesisClientLibConfiguration config,
      AmazonDynamoDBStreamsAdapterClient streamsClient) {
    return new DynamoDBStreamsProxy.Builder(
        config.getStreamName(),
        streamsClient)
        .build();
  }

  public static Scheduler createDynamoDbStreamsWorker(
      ShardRecordProcessorFactory recordProcessorFactory,
      KinesisClientLibConfiguration workerConfig, AmazonDynamoDBStreamsAdapterClient adapterClient,
      DynamoDbAsyncClient dynamoDBClient, ExecutorService executorService) {
    return createDynamoDbStreamsWorkerz(recordProcessorFactory, workerConfig, adapterClient,
        dynamoDBClient, CloudWatchAsyncClient.create(), executorService);
  }
}