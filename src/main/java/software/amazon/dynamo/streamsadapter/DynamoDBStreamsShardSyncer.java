/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import java.io.Serializable;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang3.StringUtils;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.HierarchicalShardSyncer;
import software.amazon.kinesis.leases.Lease;
import software.amazon.kinesis.leases.LeaseRefresher;
import software.amazon.kinesis.leases.ShardDetector;
import software.amazon.kinesis.leases.exceptions.DependencyException;
import software.amazon.kinesis.leases.exceptions.InvalidStateException;
import software.amazon.kinesis.leases.exceptions.ProvisionedThroughputException;
import software.amazon.kinesis.metrics.MetricsScope;
import software.amazon.kinesis.retrieval.kpl.ExtendedSequenceNumber;

/**
 * This class has been copied from ShardSyncer in KinesisClientLibrary and edited slightly to enable DynamoDB Streams
 * specific behavior. It is a helper class to sync leases with shards of the DynamoDB Stream.
 * It will create new leases/activities when it discovers new DynamoDB Streams shards (bootstrap/resharding).
 * It deletes leases for shards that have been trimmed from DynamoDB Stream.
 * It also ensures that leases for shards that have been completely processed are not deleted until their children
 * shards have also been completely processed.
 */
public class DynamoDBStreamsShardSyncer extends HierarchicalShardSyncer {

    private static final Log LOG = LogFactory.getLog(DynamoDBStreamsShardSyncer.class);
    private static final String SHARD_ID_SEPARATOR = "-";

    /* This retention period mostly will protect race conditions that are triggered by shards getting sealed
     * immediately after creation. Average active lifetime of a shard is around 4 hours today, so setting retention to
     * slightly higher helps us retain the leases for shards with active lifetime close to average for investigations
     * and visibility. Lower values on the order of minutes may also work, but reduce operational auditability.
     */
    private static final Duration MIN_LEASE_RETENTION = Duration.ofHours(6);

    private final StreamsLeaseCleanupValidator leaseCleanupValidator;


    public DynamoDBStreamsShardSyncer(StreamsLeaseCleanupValidator leaseCleanupValidator) {
        this.leaseCleanupValidator = leaseCleanupValidator;
    }

    //todo for testing only it seems
    synchronized void bootstrapShardLeases(ShardDetector shardDetector,
        LeaseRefresher leaseRefresher,
        software.amazon.kinesis.common.InitialPositionInStreamExtended  initialPositionInStream,
        boolean ignoreUnexpectedChildShards)
        throws ProvisionedThroughputException, InvalidStateException, DependencyException {
        syncShardLeases(shardDetector, leaseRefresher, initialPositionInStream, ignoreUnexpectedChildShards);
    }

    /**
     * Check and create leases for any new shards (e.g. following a reshard operation).
     *
     * @param shardDetector Implementation of IKinesisProxy that would read from the underlying Stream
     * @param leaseRefresher Performs data operations on the leases table.
     * @param initialPosition Position in Stream from which to start processing.
     * @param ignoreUnexpectedChildShards Ignore some consistency checks on the shard graph.
     * @throws DependencyException Thrown when one of the dependencies throws an exception
     * @throws InvalidStateException Unexpected state, e.g. if the leases table does not exist.
     * @throws ProvisionedThroughputException Thrown on being throttled from leases table.
     */
    @Override
    public synchronized boolean checkAndCreateLeaseForNewShards(@NotNull final ShardDetector shardDetector,
        final LeaseRefresher leaseRefresher, final software.amazon.kinesis.common.InitialPositionInStreamExtended initialPosition,
        final MetricsScope scope, final boolean ignoreUnexpectedChildShards, final boolean isLeaseTableEmpty)
        throws KinesisClientLibIOException,
        ProvisionedThroughputException, InvalidStateException, DependencyException {
        syncShardLeases(shardDetector, leaseRefresher, initialPosition, ignoreUnexpectedChildShards);
        return true;//HSS always returns true as well
    }

    /**
     * Sync leases with Kinesis shards (e.g. at startup, or when we reach end of a shard).
     *
     * @param shardDetector
     * @param leaseRefresher
     * @param initialPosition
     * @param ignoreUnexpectedChildShards
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    private synchronized void syncShardLeases(ShardDetector shardDetector,
        LeaseRefresher leaseRefresher,
        software.amazon.kinesis.common.InitialPositionInStreamExtended initialPosition,
        boolean ignoreUnexpectedChildShards)
        throws
        ProvisionedThroughputException,
        InvalidStateException,
        DependencyException {
        LOG.info("syncShardLeases: begin");
        List<Shard> shards = getShardList(shardDetector);
        LOG.debug("Num shards: " + shards.size());

        Map<String, Shard> shardIdToShardMap = constructShardIdToShardMap(shards);
        Map<String, Set<String>> shardIdToChildShardIdsMap = constructShardIdToChildShardIdsMap(shardIdToShardMap);
        Set<String> inconsistentShardIds = findInconsistentShardIds(shardIdToChildShardIdsMap, shardIdToShardMap);
        if (!ignoreUnexpectedChildShards) {
            assertAllParentShardsAreClosed(inconsistentShardIds);
        }

        List<Lease> currentLeases = leaseRefresher.listLeases();

        List<Lease> newLeasesToCreate = determineNewLeasesToCreate(shards, currentLeases, initialPosition,
            inconsistentShardIds);
        LOG.debug("Num new leases to create: " + newLeasesToCreate.size());
        for (Lease lease : newLeasesToCreate) {
            //long startTimeMillis = System.currentTimeMillis();
            try {
                boolean success = leaseRefresher.createLeaseIfNotExists(lease);
            } finally {
                //TODO fix metrics?
                //MetricsHelper.addSuccessAndLatency("CreateLease", startTimeMillis, success, MetricsLevel.DETAILED);
            }
        }

        List<Lease> trackedLeases = new ArrayList<>();
        if (!currentLeases.isEmpty()) {
            trackedLeases.addAll(currentLeases);
        }
        trackedLeases.addAll(newLeasesToCreate);
        cleanupGarbageLeases(shards, trackedLeases, shardDetector, leaseRefresher);
        //if (cleanupLeasesOfCompletedShards) {
            //todo do this always?
            cleanupLeasesOfFinishedShards(currentLeases,
                shardIdToShardMap,
                shardIdToChildShardIdsMap,
                trackedLeases,
                leaseRefresher);

        LOG.info("syncShardLeases: done");
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /** Helper method to detect a race conditiocn between fetching the shards via paginated DescribeStream calls
     * and a reshard operation.
     * @param inconsistentShardIds
     */
    private void assertAllParentShardsAreClosed(Set<String> inconsistentShardIds) {
        if (!inconsistentShardIds.isEmpty()) {
            String ids = StringUtils.join(inconsistentShardIds, ' ');
            throw new KinesisClientLibIOException(String.format("%d open child shards (%s) are inconsistent. "
                    + "This can happen due to a race condition between describeStream and a reshard operation.",
                inconsistentShardIds.size(), ids));
        }
    }

    /**
     * Helper method to construct the list of inconsistent shards, which are open shards with non-closed ancestor
     * parent(s).
     * @param shardIdToChildShardIdsMap
     * @param shardIdToShardMap
     * @return Set of inconsistent open shard ids for shards having open parents.
     */
    private Set<String> findInconsistentShardIds(Map<String, Set<String>> shardIdToChildShardIdsMap,
        Map<String, Shard> shardIdToShardMap) {
        Set<String> result = new HashSet<String>();
        for (String parentShardId : shardIdToChildShardIdsMap.keySet()) {
            Shard parentShard = shardIdToShardMap.get(parentShardId);
            if ((parentShardId == null) || (parentShard.sequenceNumberRange().endingSequenceNumber() == null)) {
                Set<String> childShardIdsMap = shardIdToChildShardIdsMap.get(parentShardId);
                result.addAll(childShardIdsMap);
            }
        }
        return result;
    }

    /**
     * Helper method to create a shardId->KinesisClientLease map.
     * Note: This has package level access for testing purposes only.
     * @param trackedLeaseList
     * @return
     */
    Map<String, Lease> constructShardIdToKCLLeaseMap(List<Lease> trackedLeaseList) {
        Map<String, Lease> trackedLeasesMap = new HashMap<>();
        for (Lease lease : trackedLeaseList) {
            trackedLeasesMap.put(lease.leaseKey(), lease);
        }
        return trackedLeasesMap;
    }

    /**
     * Note: this has package level access for testing purposes.
     * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
     * We verify that if the shard is present in the shard list, it is closed and its hash key range
     * is covered by its child shards.
     */
    synchronized void assertClosedShardsAreCoveredOrAbsent(Map<String, Shard> shardIdToShardMap,
        Map<String, Set<String>> shardIdToChildShardIdsMap,
        Set<String> shardIdsOfClosedShards) throws KinesisClientLibIOException {
        String exceptionMessageSuffix = "This can happen if we constructed the list of shards "
            + " while a reshard operation was in progress.";

        for (String shardId : shardIdsOfClosedShards) {
            Shard shard = shardIdToShardMap.get(shardId);
            if (shard == null) {
                LOG.info("Shard " + shardId + " is not present in Kinesis anymore.");
                continue;
            }

            String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
            if (endingSequenceNumber == null) {
                throw new KinesisClientLibIOException("Shard " + shardId
                    + " is not closed. " + exceptionMessageSuffix);
            }

            Set<String> childShardIds = shardIdToChildShardIdsMap.get(shardId);
            if (childShardIds == null) {
                throw new KinesisClientLibIOException("Incomplete shard list: Closed shard " + shardId
                    + " has no children." + exceptionMessageSuffix);
            }

            assertHashRangeOfClosedShardIsCovered(shard, shardIdToShardMap, childShardIds);
        }
    }

    private synchronized void assertHashRangeOfClosedShardIsCovered(Shard closedShard,
        Map<String, Shard> shardIdToShardMap,
        Set<String> childShardIds) throws KinesisClientLibIOException {

        BigInteger startingHashKeyOfClosedShard = new BigInteger(closedShard.hashKeyRange().startingHashKey());
        BigInteger endingHashKeyOfClosedShard = new BigInteger(closedShard.hashKeyRange().endingHashKey());
        BigInteger minStartingHashKeyOfChildren = null;
        BigInteger maxEndingHashKeyOfChildren = null;

        for (String childShardId : childShardIds) {
            Shard childShard = shardIdToShardMap.get(childShardId);
            BigInteger startingHashKey = new BigInteger(childShard.hashKeyRange().startingHashKey());
            if ((minStartingHashKeyOfChildren == null)
                || (startingHashKey.compareTo(minStartingHashKeyOfChildren) < 0)) {
                minStartingHashKeyOfChildren = startingHashKey;
            }
            BigInteger endingHashKey = new BigInteger(childShard.hashKeyRange().endingHashKey());
            if ((maxEndingHashKeyOfChildren == null)
                || (endingHashKey.compareTo(maxEndingHashKeyOfChildren) > 0)) {
                maxEndingHashKeyOfChildren = endingHashKey;
            }
        }

        if ((minStartingHashKeyOfChildren == null) || (maxEndingHashKeyOfChildren == null)
            || (minStartingHashKeyOfChildren.compareTo(startingHashKeyOfClosedShard) > 0)
            || (maxEndingHashKeyOfChildren.compareTo(endingHashKeyOfClosedShard) < 0)) {
            throw new KinesisClientLibIOException("Incomplete shard list: hash key range of shard "
                + closedShard.shardId() + " is not covered by its child shards.");
        }

    }

    /**
     * Helper method to construct shardId->setOfChildShardIds map.
     * Note: This has package access for testing purposes only.
     * @param shardIdToShardMap
     * @return
     */
    Map<String, Set<String>> constructShardIdToChildShardIdsMap(
        Map<String, Shard> shardIdToShardMap) {
        Map<String, Set<String>> shardIdToChildShardIdsMap = new HashMap<>();
        for (Map.Entry<String, Shard> entry : shardIdToShardMap.entrySet()) {
            String shardId = entry.getKey();
            Shard shard = entry.getValue();
            String parentShardId = shard.parentShardId();
            if ((parentShardId != null) && (shardIdToShardMap.containsKey(parentShardId))) {
              Set<String> childShardIds =
                  shardIdToChildShardIdsMap.computeIfAbsent(parentShardId, k -> new HashSet<>());
              childShardIds.add(shardId);
            }

            String adjacentParentShardId = shard.adjacentParentShardId();
            if ((adjacentParentShardId != null) && (shardIdToShardMap.containsKey(adjacentParentShardId))) {
              Set<String> childShardIds =
                  shardIdToChildShardIdsMap.computeIfAbsent(adjacentParentShardId,
                      k -> new HashSet<>());
              childShardIds.add(shardId);
            }
        }
        return shardIdToChildShardIdsMap;
    }

    private List<Shard> getShardList(ShardDetector shardDetector) throws KinesisClientLibIOException {
        LOG.info("getShardList: begin");
        List<Shard> shards = shardDetector.listShards();
        if (shards == null) {
            throw new KinesisClientLibIOException(
                "Stream is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
        }
        LOG.info("getShardList: done");
        return shards;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     *
     * For each open (no ending sequence number) shard without open parents that doesn't already have a lease,
     * determine if it is a descendent of any shard which is or will be processed (e.g. for which a lease exists):
     * If so, set checkpoint of the shard to TrimHorizon and also create leases for ancestors if needed.
     * If not, set checkpoint of the shard to the initial position specified by the client.
     * To check if we need to create leases for ancestors, we use the following rules:
     *   * If we began (or will begin) processing data for a shard, then we must reach end of that shard before
     *         we begin processing data from any of its descendants.
     *   * A shard does not start processing data until data from all its parents has been processed.
     * Note, if the initial position is LATEST and a shard has two parents and only one is a descendant - we'll create
     * leases corresponding to both the parents - the parent shard which is not a descendant will have
     * its checkpoint set to Latest.
     *
     * We assume that if there is an existing lease for a shard, then either:
     *   * we have previously created a lease for its parent (if it was needed), or
     *   * the parent shard has expired.
     *
     * For example:
     * Shard structure (each level depicts a stream segment):
     * 0 1 2 3 4   5   - shards till epoch 102
     * \ / \ / |   |
     *  6   7  4   5   - shards from epoch 103 - 205
     *   \ /   |  / \
     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
     * Current leases: (3, 4, 5)
     * New leases to create: (2, 6, 7, 8, 9, 10)
     *
     * The leases returned are sorted by the starting sequence number - following the same order
     * when persisting the leases in DynamoDB will ensure that we recover gracefully if we fail
     * before creating all the leases.
     *
     * If a shard has no existing lease, is open, and is a descendant of a parent which is still open, we ignore it
     * here; this happens when the list of shards is inconsistent, which could be due to pagination delay for very
     * high shard count streams (i.e., dynamodb streams for tables with thousands of partitions).  This can only
     * currently happen here if ignoreUnexpectedChildShards was true in syncShardleases.
     *
     *
     * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
     * @param currentLeases List of current leases
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param inconsistentShardIds Set of child shard ids having open parents.
     * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
     */
    List<Lease> determineNewLeasesToCreate(List<Shard> shards,
        List<Lease> currentLeases,
        software.amazon.kinesis.common.InitialPositionInStreamExtended initialPosition,
        Set<String> inconsistentShardIds) {
        LOG.info("determineNewLeasesToCreate: begin");
        Map<String, Lease> shardIdToNewLeaseMap = new HashMap<>();
        Map<String, Shard> shardIdToShardMapOfAllKinesisShards = constructShardIdToShardMap(shards);

        Set<String> shardIdsOfCurrentLeases = new HashSet<String>();
        for (Lease lease : currentLeases) {
            shardIdsOfCurrentLeases.add(lease.leaseKey());
            LOG.debug("Existing lease: " + lease);
        }

        List<Shard> openShards = getOpenShards(shards);
        Map<String, Boolean> memoizationContext = new HashMap<>();

        // Iterate over the open shards and find those that don't have any lease entries.
        for (Shard shard : openShards) {
            String shardId = shard.shardId();
            LOG.debug("Evaluating leases for open shard " + shardId + " and its ancestors.");
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                LOG.debug("Lease for shardId " + shardId + " already exists. Not creating a lease");
            } else if (inconsistentShardIds.contains(shardId)) {
                LOG.info("shardId " + shardId + " is an inconsistent child.  Not creating a lease");
            } else {
                LOG.debug("Need to create a lease for shardId " + shardId);
                Lease newLease = newKCLLease(shard);
                boolean isDescendant =
                    checkIfDescendantAndAddNewLeasesForAncestors(shardId,
                        initialPosition,
                        shardIdsOfCurrentLeases,
                        shardIdToShardMapOfAllKinesisShards,
                        shardIdToNewLeaseMap,
                        memoizationContext);

                /**
                 * If the shard is a descendant and the specified initial position is AT_TIMESTAMP, then the
                 * checkpoint should be set to AT_TIMESTAMP, else to TRIM_HORIZON. For AT_TIMESTAMP, we will add a
                 * lease just like we do for TRIM_HORIZON. However we will only return back records with server-side
                 * timestamp at or after the specified initial position timestamp.
                 *
                 * Shard structure (each level depicts a stream segment):
                 * 0 1 2 3 4   5   - shards till epoch 102
                 * \ / \ / |   |
                 *  6   7  4   5   - shards from epoch 103 - 205
                 *   \ /   |  /\
                 *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
                 *
                 * Current leases: empty set
                 *
                 * For the above example, suppose the initial position in stream is set to AT_TIMESTAMP with
                 * timestamp value 206. We will then create new leases for all the shards (with checkpoint set to
                 * AT_TIMESTAMP), including the ancestor shards with epoch less than 206. However as we begin
                 * processing the ancestor shards, their checkpoints would be updated to SHARD_END and their leases
                 * would then be deleted since they won't have records with server-side timestamp at/after 206. And
                 * after that we will begin processing the descendant shards with epoch at/after 206 and we will
                 * return the records that meet the timestamp requirement for these shards.
                 */
                if (isDescendant && !initialPosition.getInitialPositionInStream()
                    .equals(software.amazon.kinesis.common.InitialPositionInStream.AT_TIMESTAMP)) {
                    newLease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                } else {
                    newLease.checkpoint(convertToCheckpoint(initialPosition));
                }
                LOG.debug("Set checkpoint of " + newLease.leaseKey() + " to " + newLease.checkpoint());
                shardIdToNewLeaseMap.put(shardId, newLease);
            }
        }

      List<Lease> newLeasesToCreate = new ArrayList<>(shardIdToNewLeaseMap.values());
        Comparator<? super Lease> startingSequenceNumberComparator =
            new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards);
        newLeasesToCreate.sort(startingSequenceNumberComparator);
        LOG.info("determineNewLeasesToCreate: done");
        return newLeasesToCreate;
    }

    /**
     * Determine new leases to create and their initial checkpoint.
     * Note: Package level access only for testing purposes.
     */
    List<Lease> determineNewLeasesToCreate(List<Shard> shards,
        List<Lease> currentLeases,
        software.amazon.kinesis.common.InitialPositionInStreamExtended initialPosition) {
        Set<String> inconsistentShardIds = new HashSet<String>();
        return determineNewLeasesToCreate(shards, currentLeases, initialPosition, inconsistentShardIds);
    }

    /**
     * Note: Package level access for testing purposes only.
     * Check if this shard is a descendant of a shard that is (or will be) processed.
     * Create leases for the ancestors of this shard as required.
     * See javadoc of determineNewLeasesToCreate() for rules and example.
     *
     * @param shardId The shardId to check.
     * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
     *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
     * @param shardIdsOfCurrentLeases The shardIds for the current leases.
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @param shardIdToLeaseMapOfNewShards Add lease POJOs corresponding to ancestors to this map.
     * @param memoizationContext Memoization of shards that have been evaluated as part of the evaluation
     * @return true if the shard is a descendant of any current shard (lease already exists)
     */
    // CHECKSTYLE:OFF CyclomaticComplexity
    boolean checkIfDescendantAndAddNewLeasesForAncestors(String shardId,
        software.amazon.kinesis.common.InitialPositionInStreamExtended initialPosition,
        Set<String> shardIdsOfCurrentLeases,
        Map<String, Shard> shardIdToShardMapOfAllKinesisShards,
        Map<String, Lease> shardIdToLeaseMapOfNewShards,
        Map<String, Boolean> memoizationContext) {

        Boolean previousValue = memoizationContext.get(shardId);
        if (previousValue != null) {
            return previousValue;
        }

        boolean isDescendant = false;
        Shard shard;
        Set<String> parentShardIds;
        Set<String> descendantParentShardIds = new HashSet<String>();

        if ((shardId != null) && (shardIdToShardMapOfAllKinesisShards.containsKey(shardId))) {
            if (shardIdsOfCurrentLeases.contains(shardId)) {
                // This shard is a descendant of a current shard.
                isDescendant = true;
                // We don't need to add leases of its ancestors,
                // because we'd have done it when creating a lease for this shard.
            } else {
                shard = shardIdToShardMapOfAllKinesisShards.get(shardId);
                parentShardIds = getParentShardIds(shard, shardIdToShardMapOfAllKinesisShards);
                for (String parentShardId : parentShardIds) {
                    // Check if the parent is a descendant, and include its ancestors.
                    if (checkIfDescendantAndAddNewLeasesForAncestors(parentShardId,
                        initialPosition,
                        shardIdsOfCurrentLeases,
                        shardIdToShardMapOfAllKinesisShards,
                        shardIdToLeaseMapOfNewShards,
                        memoizationContext)) {
                        isDescendant = true;
                        descendantParentShardIds.add(parentShardId);
                        LOG.debug("Parent shard " + parentShardId + " is a descendant.");
                    } else {
                        LOG.debug("Parent shard " + parentShardId + " is NOT a descendant.");
                    }
                }

                // If this is a descendant, create leases for its parent shards (if they don't exist)
                if (isDescendant) {
                    for (String parentShardId : parentShardIds) {
                        if (!shardIdsOfCurrentLeases.contains(parentShardId)) {
                            LOG.debug("Need to create a lease for shardId " + parentShardId);
                            Lease lease = shardIdToLeaseMapOfNewShards.get(parentShardId);
                            if (lease == null) {
                                lease = newKCLLease(shardIdToShardMapOfAllKinesisShards.get(parentShardId));
                                shardIdToLeaseMapOfNewShards.put(parentShardId, lease);
                            }

                            if (descendantParentShardIds.contains(parentShardId)
                                && !initialPosition.getInitialPositionInStream()
                                .equals(software.amazon.kinesis.common.InitialPositionInStream.AT_TIMESTAMP)) {
                                lease.checkpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                            } else {
                                lease.checkpoint(convertToCheckpoint(initialPosition));
                            }
                        }
                    }
                } else {
                    // This shard should be included, if the customer wants to process all records in the stream or
                    // if the initial position is AT_TIMESTAMP. For AT_TIMESTAMP, we will add a lease just like we do
                    // for TRIM_HORIZON. However, we will only return back records with server-side timestamp at or
                    // after the specified initial position timestamp.
                    if (initialPosition.getInitialPositionInStream().equals(software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON)
                        || initialPosition.getInitialPositionInStream()
                        .equals(software.amazon.kinesis.common.InitialPositionInStream.AT_TIMESTAMP)) {
                        isDescendant = true;
                    }
                }

            }
        }

        memoizationContext.put(shardId, isDescendant);
        return isDescendant;
    }
    // CHECKSTYLE:ON CyclomaticComplexity

    /**
     * Helper method to get parent shardIds of the current shard - includes the parent shardIds if:
     * a/ they are not null
     * b/ if they exist in the current shard map (i.e. haven't  expired)
     *
     * @param shard Will return parents of this shard
     * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
     * @return Set of parentShardIds
     */
    Set<String> getParentShardIds(Shard shard, Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
        Set<String> parentShardIds = new HashSet<String>(2);
        String parentShardId = shard.parentShardId();
        if ((parentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(parentShardId)) {
            parentShardIds.add(parentShardId);
        }
        String adjacentParentShardId = shard.adjacentParentShardId();
        if ((adjacentParentShardId != null) && shardIdToShardMapOfAllKinesisShards.containsKey(adjacentParentShardId)) {
            parentShardIds.add(adjacentParentShardId);
        }
        return parentShardIds;
    }

    /**
     * Delete leases corresponding to shards that no longer exist in the stream.
     * Current scheme: Delete a lease if:
     *   * the corresponding shard is not present in the list of Kinesis shards, AND
     *   * the parentShardIds listed in the lease are also not present in the list of Kinesis shards.
     * @param shards List of all Kinesis shards (assumed to be a consistent snapshot - when stream is in Active state).
     * @param trackedLeases List of
     * @param shardDetector Kinesis proxy (used to get shard list)
     * @param leaseRefresher
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException Thrown if we couldn't get a fresh shard list from Kinesis.
     * @throws DependencyException
     */
    private void cleanupGarbageLeases(List<Shard> shards,
        List<Lease> trackedLeases,
        ShardDetector shardDetector,
        LeaseRefresher leaseRefresher)
        throws
        ProvisionedThroughputException,
        InvalidStateException,
        DependencyException {
        LOG.info("cleanupGarbageLeases: begin");
        Set<String> kinesisShards = new HashSet<>();
        for (Shard shard : shards) {
            kinesisShards.add(shard.shardId());
        }

        // Check if there are leases for non-existent shards
        List<Lease> garbageLeases = new ArrayList<>();
        for (Lease lease : trackedLeases) {
            if (leaseCleanupValidator.isCandidateForCleanup(lease, kinesisShards)) {
                garbageLeases.add(lease);
            }
        }

        if (!garbageLeases.isEmpty()) {
            LOG.info("Found " + garbageLeases.size()
                + " candidate leases for cleanup. Refreshing list of"
                + " Kinesis shards to pick up recent/latest shards");
            List<Shard> currentShardList = getShardList(shardDetector);
            Set<String> currentKinesisShardIds = new HashSet<>();
            for (Shard shard : currentShardList) {
                currentKinesisShardIds.add(shard.shardId());
            }

            for (Lease lease : garbageLeases) {
                if (leaseCleanupValidator.isCandidateForCleanup(lease, currentKinesisShardIds)) {
                    LOG.info("Deleting lease for shard " + lease.leaseKey()
                        + " as it is not present in Kinesis stream.");
                    leaseRefresher.deleteLease(lease);
                }
            }
        }
        LOG.info("cleanupGarbageLeases: done");
    }

    /**
     * Private helper method.
     * Clean up leases for shards that meet the following criteria:
     * a/ the shard has been fully processed (checkpoint is set to SHARD_END)
     * b/ we've begun processing all the child shards: we have leases for all child shards and their checkpoint is not
     *      TRIM_HORIZON.
     *
     * @param currentLeases List of leases we evaluate for clean up
     * @param shardIdToShardMap Map of shardId->Shard (assumed to include all Kinesis shards)
     * @param shardIdToChildShardIdsMap Map of shardId->childShardIds (assumed to include all Kinesis shards)
     * @param trackedLeases List of all leases we are tracking.
     * @param leaseRefresher Lease manager (will be used to delete leases)
     * @throws DependencyException
     * @throws InvalidStateException
     * @throws ProvisionedThroughputException
     * @throws KinesisClientLibIOException
     */
    private synchronized void cleanupLeasesOfFinishedShards(Collection<Lease> currentLeases,
        Map<String, Shard> shardIdToShardMap,
        Map<String, Set<String>> shardIdToChildShardIdsMap,
        List<Lease> trackedLeases,
        LeaseRefresher leaseRefresher)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        LOG.info("cleanupLeasesOfFinishedShards: begin");
        Set<String> shardIdsOfClosedShards = new HashSet<>();
        List<Lease> leasesOfClosedShards = new ArrayList<>();
        for (Lease lease : currentLeases) {
            if (lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
                shardIdsOfClosedShards.add(lease.leaseKey());
                leasesOfClosedShards.add(lease);
            }
        }

        if (!leasesOfClosedShards.isEmpty()) {
            assertClosedShardsAreCoveredOrAbsent(shardIdToShardMap,
                shardIdToChildShardIdsMap,
                shardIdsOfClosedShards);
            Comparator<? super Lease> startingSequenceNumberComparator
                = new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMap);
            leasesOfClosedShards.sort(startingSequenceNumberComparator);
            Map<String, Lease> trackedLeaseMap = constructShardIdToKCLLeaseMap(trackedLeases);

            for (Lease leaseOfClosedShard : leasesOfClosedShards) {
                String closedShardId = leaseOfClosedShard.leaseKey();
                Set<String> childShardIds = shardIdToChildShardIdsMap.get(closedShardId);
                if ((closedShardId != null) && (childShardIds != null) && (!childShardIds.isEmpty())) {
                    cleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, leaseRefresher);
                }
            }
        }
        LOG.info("cleanupLeasesOfFinishedShards: done");
    }

    /**
     * Delete lease for the closed shard. Rules for deletion are:
     * a/ the checkpoint for the closed shard is SHARD_END,
     * b/ there are leases for all the childShardIds and their checkpoint is NOT TRIM_HORIZON
     * Note: This method has package level access solely for testing purposes.
     *
     * @param closedShardId Identifies the closed shard
     * @param childShardIds ShardIds of children of the closed shard
     * @param trackedLeases shardId->KinesisClientLease map with all leases we are tracking (should not be null)
     * @param leaseRefresher
     * @throws ProvisionedThroughputException
     * @throws InvalidStateException
     * @throws DependencyException
     */
    synchronized void cleanupLeaseForClosedShard(String closedShardId,
        Set<String> childShardIds,
        Map<String, Lease> trackedLeases,
      LeaseRefresher leaseRefresher)
        throws DependencyException, InvalidStateException, ProvisionedThroughputException {
        Lease leaseForClosedShard = trackedLeases.get(closedShardId);
        List<Lease> childShardLeases = new ArrayList<>();

        for (String childShardId : childShardIds) {
            Lease childLease = trackedLeases.get(childShardId);
            if (childLease != null) {
                childShardLeases.add(childLease);
            }
        }

        if ((leaseForClosedShard != null)
            && (leaseForClosedShard.checkpoint().equals(ExtendedSequenceNumber.SHARD_END))
            && (childShardLeases.size() == childShardIds.size())) {
            boolean okayToDelete = true;
            for (Lease lease : childShardLeases) {
                if (!lease.checkpoint().equals(ExtendedSequenceNumber.SHARD_END)) {
                    okayToDelete = false; // if any child is still being processed, don't delete lease for parent
                    break;
                }
            }

            try {
                if (Instant.now().isBefore(getShardCreationTime(closedShardId).plus(MIN_LEASE_RETENTION))) {
                    okayToDelete = false; // if parent was created within lease retention period, don't delete lease for parent
                }
            } catch (RuntimeException e) {
                LOG.info("Could not extract creation time from ShardId [" + closedShardId +"]");
                LOG.debug(e);
            }

            if (okayToDelete) {
                LOG.info("Deleting lease for shard " + leaseForClosedShard.leaseKey()
                    + " as it is eligible for cleanup - its child shard is check-pointed at SHARD_END.");
                leaseRefresher.deleteLease(leaseForClosedShard);
            }
        }
    }

    /**
     * This method extracts the shard creation time from the ShardId
     *
     * @param shardId
     * @return instant at which the shard was created
     */
    private Instant getShardCreationTime(String shardId) {
        return Instant.ofEpochMilli(Long.parseLong(shardId.split(SHARD_ID_SEPARATOR)[1]));
    }

    /**
     * Helper method to create a new KinesisClientLease POJO for a shard.
     * Note: Package level access only for testing purposes
     *
     * @param shard
     * @return
     */
    Lease newKCLLease(Shard shard) {
        List<String> parentShardIds = new ArrayList<String>(2);
        if (shard.parentShardId() != null) {
            parentShardIds.add(shard.parentShardId());
        }
        if (shard.adjacentParentShardId() != null) {
            parentShardIds.add(shard.adjacentParentShardId());
        }
        Lease newLease = new Lease();
        newLease.leaseKey(shard.shardId());
        newLease.parentShardIds(parentShardIds);
        newLease.ownerSwitchesSinceCheckpoint(0L);

        return newLease;
    }

    /**
     * Helper method to construct a shardId->Shard map for the specified list of shards.
     *
     * @param shards List of shards
     * @return ShardId->Shard map
     */
    Map<String, Shard> constructShardIdToShardMap(List<Shard> shards) {
        return shards.stream().collect(Collectors.toMap(Shard::shardId, Function.identity()));
    }

    /**
     * Helper method to return all the open shards for a stream.
     * Note: Package level access only for testing purposes.
     *
     * @param allShards All shards returved via DescribeStream. We assume this to represent a consistent shard list.
     * @return List of open shards (shards at the tip of the stream) - may include shards that are not yet active.
     */
    List<Shard> getOpenShards(List<Shard> allShards) {
        List<Shard> openShards = new ArrayList<>();
        for (Shard shard : allShards) {
            String endingSequenceNumber = shard.sequenceNumberRange().endingSequenceNumber();
            if (endingSequenceNumber == null) {
                openShards.add(shard);
                LOG.debug("Found open shard: " + shard.shardId());
            }
        }
        return openShards;
    }

    private ExtendedSequenceNumber convertToCheckpoint(software.amazon.kinesis.common.InitialPositionInStreamExtended position) {
        ExtendedSequenceNumber checkpoint = null;

        if (position.getInitialPositionInStream().equals(software.amazon.kinesis.common.InitialPositionInStream.TRIM_HORIZON)) {
            checkpoint = ExtendedSequenceNumber.TRIM_HORIZON;
        } else if (position.getInitialPositionInStream().equals(software.amazon.kinesis.common.InitialPositionInStream.LATEST)) {
            checkpoint = ExtendedSequenceNumber.LATEST;
        } else if (position.getInitialPositionInStream().equals(software.amazon.kinesis.common.InitialPositionInStream.AT_TIMESTAMP)) {
            checkpoint = ExtendedSequenceNumber.AT_TIMESTAMP;
        }

        return checkpoint;
    }

    /** Helper class to compare leases based on starting sequence number of the corresponding shards.
     *
     */
    private static class StartingSequenceNumberAndShardIdBasedComparator implements Comparator<Lease>,
        Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, Shard> shardIdToShardMap;

        /**
         * @param shardIdToShardMapOfAllKinesisShards
         */
        public StartingSequenceNumberAndShardIdBasedComparator(Map<String, Shard> shardIdToShardMapOfAllKinesisShards) {
            shardIdToShardMap = shardIdToShardMapOfAllKinesisShards;
        }

        /**
         * Compares two leases based on the starting sequence number of corresponding shards.
         * If shards are not found in the shardId->shard map supplied, we do a string comparison on the shardIds.
         * We assume that lease1 and lease2 are:
         *     a/ not null,
         *     b/ shards (if found) have non-null starting sequence numbers
         *
         * {@inheritDoc}
         */
        @Override
        public int compare(Lease lease1, Lease lease2) {
            int result = 0;
            String shardId1 = lease1.leaseKey();
            String shardId2 = lease2.leaseKey();
            Shard shard1 = shardIdToShardMap.get(shardId1);
            Shard shard2 = shardIdToShardMap.get(shardId2);

            // If we found shards for the two leases, use comparison of the starting sequence numbers
            if ((shard1 != null) && (shard2 != null)) {
                BigInteger sequenceNumber1 =
                    new BigInteger(shard1.sequenceNumberRange().startingSequenceNumber());
                BigInteger sequenceNumber2 =
                    new BigInteger(shard2.sequenceNumberRange().startingSequenceNumber());
                result = sequenceNumber1.compareTo(sequenceNumber2);
            }

            if (result == 0) {
                result = shardId1.compareTo(shardId2);
            }

            return result;
        }

    }

}