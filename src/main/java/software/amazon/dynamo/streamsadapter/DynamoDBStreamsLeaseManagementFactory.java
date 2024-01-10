package software.amazon.dynamo.streamsadapter;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.Tag;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.LeaseCleanupConfig;
import software.amazon.kinesis.common.StreamConfig;
import software.amazon.kinesis.coordinator.DeletedStreamListProvider;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.LeaseSerializer;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.ShardSyncTaskManager;
import software.amazon.kinesis.leases.dynamodb.DynamoDBLeaseManagementFactory;
import software.amazon.kinesis.leases.dynamodb.TableCreatorCallback;
import software.amazon.kinesis.metrics.MetricsFactory;

public class DynamoDBStreamsLeaseManagementFactory extends DynamoDBLeaseManagementFactory {
  public DynamoDBStreamsLeaseManagementFactory(
      KinesisAsyncClient kinesisClient,
      DynamoDbAsyncClient dynamoDBClient, String tableName,
      String workerIdentifier, ExecutorService executorService,
      long failoverTimeMillis, long epsilonMillis, int maxLeasesForWorker,
      int maxLeasesToStealAtOneTime, int maxLeaseRenewalThreads,
      boolean cleanupLeasesUponShardCompletion, boolean ignoreUnexpectedChildShards,
      long shardSyncIntervalMillis, boolean consistentReads, long listShardsBackoffTimeMillis,
      int maxListShardsRetryAttempts, int maxCacheMissesBeforeReload,
      long listShardsCacheAllowedAgeInSeconds, int cacheMissWarningModulus,
      long initialLeaseTableReadCapacity, long initialLeaseTableWriteCapacity,
      HierarchicalShardSyncer deprecatedHierarchicalShardSyncer,
      TableCreatorCallback tableCreatorCallback,
      Duration dynamoDbRequestTimeout,
      BillingMode billingMode,
      Collection<Tag> tags,
      LeaseSerializer leaseSerializer,
      Function<StreamConfig, ShardDetector> customShardDetectorProvider,
      boolean isMultiStreamMode,
      LeaseCleanupConfig leaseCleanupConfig) {
    super(kinesisClient, dynamoDBClient, tableName, workerIdentifier, executorService,
        failoverTimeMillis, epsilonMillis, maxLeasesForWorker, maxLeasesToStealAtOneTime,
        maxLeaseRenewalThreads, cleanupLeasesUponShardCompletion, ignoreUnexpectedChildShards,
        shardSyncIntervalMillis, consistentReads, listShardsBackoffTimeMillis,
        maxListShardsRetryAttempts, maxCacheMissesBeforeReload, listShardsCacheAllowedAgeInSeconds,
        cacheMissWarningModulus, initialLeaseTableReadCapacity, initialLeaseTableWriteCapacity,
        deprecatedHierarchicalShardSyncer, tableCreatorCallback, dynamoDbRequestTimeout,
        billingMode,
        tags, leaseSerializer, customShardDetectorProvider, isMultiStreamMode, leaseCleanupConfig);
  }


  /**
   * Create ShardSyncTaskManager from the streamConfig passed
   *
   * @param metricsFactory - factory to get metrics object
   * @param streamConfig - streamConfig for which ShardSyncTaskManager needs to be created
   * @param deletedStreamListProvider - store for capturing the streams which are deleted in kinesis
   * @return ShardSyncTaskManager
   */
  @Override
  public ShardSyncTaskManager createShardSyncTaskManager(MetricsFactory metricsFactory, StreamConfig streamConfig,
      DeletedStreamListProvider deletedStreamListProvider) {
    return new ShardSyncTaskManager(this.createShardDetector(streamConfig),
        this.createLeaseRefresher(),
        streamConfig.initialPositionInStreamExtended(),
        this.isCleanupLeasesUponShardCompletion(),
        this.isIgnoreUnexpectedChildShards(),
        this.getShardSyncIntervalMillis(),
        this.getExecutorService(),
        //todo need the deletedstreamlistprovider?
        new DynamoDBStreamsShardSyncer(new StreamsLeaseCleanupValidator()),
        metricsFactory);
  }
}