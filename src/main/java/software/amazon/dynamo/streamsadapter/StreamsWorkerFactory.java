/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseManagementConfig;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseSerializer;
import software.amazon.kinesis.leases.dynamodb.DynamoDBMultiStreamLeaseSerializer;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.coordinator.KinesisClientLibConfiguration;
import software.amazon.kinesis.retrieval.RetrievalConfig;

/**
 * The StreamsWorkerFactory uses the Kinesis Client Library's Worker
 * class to provide convenient constructors for ease-of-use.
 */
public class StreamsWorkerFactory {
    private static final Log LOG = LogFactory.getLog(StreamsWorkerFactory.class);

    /**
     * @param recordProcessorFactory Used to get record processor instances for processing data from shards
     * @param config                 Kinesis Client Library configuration
     * @param streamsClient          DynamoDB Streams Adapter Client used for fetching data
     * @param dynamoDBClient         DynamoDB client used for checkpoints and tracking leases
     * @param metricsFactory         Metrics factory used to emit metrics
     * @param execService            ExecutorService to use for processing records (support for multi-threaded
     *                               consumption)
     * @return                       An instance of KCL worker injected with DynamoDB Streams specific dependencies.
     */
    public static Scheduler createDynamoDbStreamsWorkerz(
        ShardRecordProcessorFactory shardRecordProcessorFactory,
        KinesisClientLibConfiguration config,
        AmazonDynamoDBStreamsAdapterClient streamsClient,
        DynamoDbAsyncClient dynamoDBClient,
        CloudWatchAsyncClient cloudWatchClient) {

        //todo UUID??
        ConfigsBuilder configsBuilder = new ConfigsBuilder(config.getStreamName(), config.getApplicationName(), streamsClient, dynamoDBClient, cloudWatchClient, UUID.randomUUID().toString(), shardRecordProcessorFactory);

        DynamoDBStreamsProxy dynamoDBStreamsProxy = getDynamoDBStreamsProxy(config, streamsClient);

        HierarchicalShardSyncer shardSyncer = new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator());

      LeaseManagementConfig lmc = configsBuilder.leaseManagementConfig();
      lmc = lmc.hierarchicalShardSyncer(shardSyncer);//DOESNT WORK
      lmc = lmc.customShardDetectorProvider(cfg -> dynamoDBStreamsProxy);//todo do we care about the stream name coming from cfg here?


      DynamoDBStreamsLeaseManagementFactory dynamoDBStreamsLeaseManagementFactory = new DynamoDBStreamsLeaseManagementFactory(
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
          shardSyncer,
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
      //Required to override the HierarchialShardSyncer passed  into ShardSyncTaskManager in createShardSyncTaskManager
      // this.shardSyncTaskManagerProvider = streamConfig -> this.leaseManagementConfig
      //                .leaseManagementFactory(leaseSerializer, isMultiStreamMode)
      //                .createShardSyncTaskManager(this.metricsFactory, streamConfig, this.deletedStreamListProvider);
      //ShardSyncTaskManager owns the shardsyncer and the refresher?

      //Need custom retrieval factory to prevent fanout and lots of stream accessor calls to kinesisasycnclient (not impl'd)

      RetrievalConfig rc = configsBuilder.retrievalConfig();
      //rc = rc.retrievalFactory(new DynamoDBStreamsRetrievalFactory(
      //    streamsClient,
      //    config.getStreamName(),
      //    "defaultconsumerArn",
      //    streamName -> streamName//todo arn converter, not correct
      //));
      ////todo ^^

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

    public static Scheduler createDynamoDbStreamsWorker(ShardRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration workerConfig, AmazonDynamoDBStreamsAdapterClient adapterClient, DynamoDbAsyncClient dynamoDBClient, ExecutorService executorService) {
        return createDynamoDbStreamsWorkerz(recordProcessorFactory, workerConfig, adapterClient, dynamoDBClient, CloudWatchAsyncClient.create());
    }
}