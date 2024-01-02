/*
 * Copyright 2014-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.dynamo.streamsadapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;
import software.amazon.kinesis.exceptions.internal.KinesisClientLibIOException;
import software.amazon.kinesis.leases.Lease;

/**
 * Decides if a lease is eligible for cleanup.
 */
//TODO REMOVED FROM SHARDSYNCER IN 2x
public class StreamsLeaseCleanupValidator  {

    private static final Log LOG = LogFactory.getLog(StreamsLeaseCleanupValidator.class);

    /**
     * @param lease Candidate shard we are considering for deletion.
     * @param currentKinesisShardIds List of leases currently held by the worker.
     * @return true if neither the shard (corresponding to the lease), nor its parents are present in
     *         currentKinesisShardIds
     * @throws KinesisClientLibIOException Thrown if currentKinesisShardIds contains a parent shard but not the child
     *         shard (we are evaluating for deletion).
     */
    public boolean isCandidateForCleanup(Lease lease, Set<String> currentKinesisShardIds) throws KinesisClientLibIOException {
        boolean isCandidateForCleanup = true;

        if (currentKinesisShardIds.contains(lease.leaseKey())) {
            isCandidateForCleanup = false;
        } else {
            LOG.info("Found lease for non-existent shard: " + lease.leaseKey() + ". Checking its parent shards");
            Set<String> parentShardIds = lease.parentShardIds();
            for (String parentShardId : parentShardIds) {

                // Throw an exception if the parent shard exists (but the child does not).
                // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards.
                if (currentKinesisShardIds.contains(parentShardId)) {
                    String message =
                        "Parent shard " + parentShardId + " exists but not the child shard "
                            + lease.leaseKey();
                    LOG.info(message);
                    throw new KinesisClientLibIOException(message);
                }
            }
        }

        return isCandidateForCleanup;
    }
}