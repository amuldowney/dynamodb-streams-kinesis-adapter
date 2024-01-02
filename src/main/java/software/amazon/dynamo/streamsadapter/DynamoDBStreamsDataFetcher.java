//package software.amazon.dynamo.streamsadapter;
//
//import java.util.Collections;
//import java.util.Date;
//import java.util.List;
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import com.amazonaws.util.CollectionUtils;
//import com.google.common.collect.Iterables;
//import lombok.Data;
//import org.reactivestreams.Subscriber;
//import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
//import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
//import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
//import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
//import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
//import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
//import software.amazon.kinesis.checkpoint.SentinelCheckpoint;
//import software.amazon.kinesis.common.InitialPositionInStreamExtended;
//import software.amazon.kinesis.common.RequestDetails;
//import software.amazon.kinesis.leases.ShardInfo;
//import software.amazon.kinesis.retrieval.DataFetcherResult;
//import software.amazon.kinesis.retrieval.RecordsDeliveryAck;
//import software.amazon.kinesis.retrieval.RecordsPublisher;
//import software.amazon.kinesis.retrieval.RecordsRetrieved;
//import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;
//
///**
// * A combination of the old IDataFetcher and the IShardConsumer
// */
//public class DynamoDBStreamsDataFetcher implements RecordsPublisher {
//    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsDataFetcher.class);
//
//    private String nextIterator;
//    private KinesisAsyncClient kinesisProxy;
//    private final String shardId;
//    private boolean isShardEndReached;
//    private boolean isInitialized;
//    private String lastKnownSequenceNumber;
//    private InitialPositionInStreamExtended initialPositionInStream;
//
//    /**
//     *
//     * @param kinesisProxy Kinesis proxy
//     * @param shardInfo The shardInfo object.
//     */
//    public DynamoDBStreamsDataFetcher(KinesisAsyncClient kinesisProxy, ShardInfo shardInfo) {
//        this.shardId = shardInfo.shardId();
//        this.kinesisProxy = kinesisProxy;
//    }
//
//    /**
//     * Get records from the current position in the stream (up to maxRecords).
//     *
//     * @param maxRecords Max records to fetch
//     * @return list of records of up to maxRecords size
//     */
//    public DataFetcherResult getRecords(int maxRecords) {
//        if (!isInitialized) {
//            throw new IllegalArgumentException("KinesisDataFetcher.getRecords called before initialization.");
//        }
//
//        if (nextIterator != null) {
//            try {
//                GetRecordsRequest request = GetRecordsRequest.builder().shardIterator(nextIterator).limit(maxRecords).build();
//                return new DynamoDBStreamsDataFetcher.AdvancingResult(kinesisProxy.getRecords(request).join());
//            } catch (ResourceNotFoundException e) {
//                LOG.info("Caught ResourceNotFoundException when fetching records for shard " + shardId);
//                return TERMINAL_RESULT;
//            }
//        } else {
//            LOG.info("Skipping fetching records from Kinesis for shard " + shardId + ": nextIterator is null.");
//            return TERMINAL_RESULT;
//        }
//    }
//
//    final DataFetcherResult TERMINAL_RESULT = new DataFetcherResult() {
//        @Override
//        public GetRecordsResponse getResult() {
//            return GetRecordsResponse.builder()
//                    .millisBehindLatest(null)
//                    .records(Collections.emptyList())
//                    .nextShardIterator(null)
//                .build();
//        }
//
//        @Override
//        public GetRecordsResponse accept() {
//            isShardEndReached = true;
//            return getResult();
//        }
//
//        @Override
//        public boolean isShardEnd() {
//            return isShardEndReached;
//        }
//    };
//
//
//    //TODO IMPL =================
//    @Override public void restartFrom(RecordsRetrieved recordsRetrieved) {
//
//    }
//
//    @Override public void shutdown() {
//
//    }
//
//    @Override public RequestDetails getLastSuccessfulRequestDetails() {
//        return null;
//    }
//
//    @Override public void notify(RecordsDeliveryAck ack) {
//        RecordsPublisher.super.notify(ack);
//    }
//
//    @Override public void subscribe(Subscriber<? super RecordsRetrieved> s) {
//        throw new RuntimeException("BROKEN!!!!");
//    }
//
//    @Data
//    class AdvancingResult implements DataFetcherResult {
//
//        final GetRecordsResponse result;
//
//        @Override
//        public GetRecordsResponse getResult() {
//            return result;
//        }
//
//        @Override
//        public GetRecordsResponse accept() {
//            nextIterator = result.nextShardIterator();
//            if (!CollectionUtils.isNullOrEmpty(result.records())) {
//                lastKnownSequenceNumber = Iterables.getLast(result.records()).sequenceNumber();
//            }
//            if (nextIterator == null) {
//                LOG.info("Reached shard end: nextIterator is null in AdvancingResult.accept for shard " + shardId);
//
//                isShardEndReached = true;
//            }
//            return getResult();
//        }
//
//        @Override
//        public boolean isShardEnd() {
//            return isShardEndReached;
//        }
//    }
//
//    /**
//     * Initializes this KinesisDataFetcher's iterator based on the checkpointed sequence number.
//     * @param initialCheckpoint Current checkpoint sequence number for this shard.
//     * @param initialPositionInStream The initialPositionInStream.
//     */
//    @Override public void start(
//        ExtendedSequenceNumber initialCheckpoint,
//        InitialPositionInStreamExtended initialPositionInStream) {
//        LOG.info("Initializing shard " + shardId + " with " + initialCheckpoint.sequenceNumber());
//        advanceIteratorTo(initialCheckpoint.sequenceNumber(), initialPositionInStream);
//        isInitialized = true;
//    }
//
//    /**
//     * Advances this KinesisDataFetcher's internal iterator to be at the passed-in sequence number.
//     *
//     * @param sequenceNumber advance the iterator to the record at this sequence number.
//     * @param initialPositionInStream The initialPositionInStream.
//     */
//    public void advanceIteratorTo(String sequenceNumber, InitialPositionInStreamExtended initialPositionInStream) {
//        if (sequenceNumber == null) {
//            throw new IllegalArgumentException("SequenceNumber should not be null: shardId " + shardId);
//        } else if (sequenceNumber.equals(SentinelCheckpoint.LATEST.toString())) {
//            nextIterator = getIterator(ShardIteratorType.LATEST.toString());
//        } else if (sequenceNumber.equals(SentinelCheckpoint.TRIM_HORIZON.toString())) {
//            nextIterator = getIterator(ShardIteratorType.TRIM_HORIZON.toString());
//        } else if (sequenceNumber.equals(SentinelCheckpoint.AT_TIMESTAMP.toString())) {
//            nextIterator = getIterator(initialPositionInStream.getTimestamp());
//        } else if (sequenceNumber.equals(SentinelCheckpoint.SHARD_END.toString())) {
//            nextIterator = null;
//        } else {
//            nextIterator = getIterator(ShardIteratorType.AT_SEQUENCE_NUMBER.toString(), sequenceNumber);
//        }
//        if (nextIterator == null) {
//            LOG.info("Reached shard end: cannot advance iterator for shard " + shardId);
//            isShardEndReached = true;
//            // TODO: transition to ShuttingDown state on shardend instead to shutdown state for enqueueing this for cleanup
//        }
//        this.lastKnownSequenceNumber = sequenceNumber;
//        this.initialPositionInStream = initialPositionInStream;
//    }
//
//    /**
//     * @param iteratorType The iteratorType - either AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER.
//     * @param sequenceNumber The sequenceNumber.
//     *
//     * @return iterator or null if we catch a ResourceNotFound exception
//     */
//    private String getIterator(String iteratorType, String sequenceNumber) {
//        String iterator = null;
//        try {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("Calling getIterator for " + shardId + ", iterator type " + iteratorType
//                        + " and sequence number " + sequenceNumber);
//            }
//            //iterator = kinesisProxy.getIterator(shardId, iteratorType, sequenceNumber);
//            GetShardIteratorRequest request = GetShardIteratorRequest.builder().shardId(shardId).shardIteratorType(iteratorType).startingSequenceNumber(sequenceNumber).build();
//            iterator = kinesisProxy.getShardIterator(request).join().shardIterator();
//        } catch (ResourceNotFoundException e) {
//            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
//        }
//        return iterator;
//    }
//
//    /**
//     * @param iteratorType The iteratorType - either TRIM_HORIZON or LATEST.
//     * @return iterator or null if we catch a ResourceNotFound exception
//     */
//    private String getIterator(String iteratorType) {
//        String iterator = null;
//        try {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("Calling getIterator for " + shardId + " and iterator type " + iteratorType);
//            }
//            //iterator = kinesisProxy.getIterator(shardId, iteratorType);
//            GetShardIteratorRequest request = GetShardIteratorRequest.builder().shardId(shardId).shardIteratorType(iteratorType).build();
//            iterator = kinesisProxy.getShardIterator(request).join().shardIterator();
//        } catch (ResourceNotFoundException e) {
//            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
//        }
//        return iterator;
//    }
//
//    /**
//     * @param timestamp The timestamp.
//     * @return iterator or null if we catch a ResourceNotFound exception
//     */
//    private String getIterator(Date timestamp) {
//        String iterator = null;
//        try {
//            if (LOG.isDebugEnabled()) {
//                LOG.debug("Calling getIterator for " + shardId + " and timestamp " + timestamp);
//            }
//            //iterator = kinesisProxy.getIterator(shardId, timestamp);
//            GetShardIteratorRequest request = GetShardIteratorRequest.builder().shardId(shardId).timestamp(timestamp.toInstant()).build();
//            iterator = kinesisProxy.getShardIterator(request).join().shardIterator();
//        } catch (ResourceNotFoundException e) {
//            LOG.info("Caught ResourceNotFoundException when getting an iterator for shard " + shardId, e);
//        }
//        return iterator;
//    }
//
//
//    @Override public void restartFrom(RecordsRetrieved recordsRetrieved) {
//        recordsRetrieved.processRecordsInput().
//        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
//    }
//
//
//    /**
//     * Gets a new iterator from the last known sequence number i.e. the sequence number of the last record from the last
//     * getRecords call.
//     */
//    public void restartIterator() {
//        if (StringUtils.isEmpty(lastKnownSequenceNumber) || initialPositionInStream == null) {
//            throw new IllegalStateException("Make sure to initialize the KinesisDataFetcher before restarting the iterator.");
//        }
//        advanceIteratorTo(lastKnownSequenceNumber, initialPositionInStream);
//    }
//
//    /**
//     * @return the shardEndReached
//     */
//    public boolean isShardEndReached() {
//        return isShardEndReached;
//    }
//
//    //public List<ChildShard> getChildShards() {
//    //    return Collections.emptyList();
//    //}
//
//    /** Note: This method has package level access for testing purposes.
//     * @return nextIterator
//     */
//    String getNextIterator() {
//        return nextIterator;
//    }
//}