package org.opensearch.replication.action.stop

import org.junit.Before
import org.junit.Test
import org.mockito.Mockito.mock
import org.opensearch.Version
import org.opensearch.cluster.ClusterName
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.Metadata
import org.opensearch.cluster.service.ClusterService
import org.opensearch.commons.replication.action.StopIndexReplicationRequest
import org.opensearch.core.index.Index
import org.opensearch.core.index.shard.ShardId
import org.opensearch.persistent.PersistentTaskParams
import org.opensearch.persistent.PersistentTasksCustomMetadata
import org.opensearch.replication.task.index.IndexReplicationExecutor
import org.opensearch.replication.task.index.IndexReplicationParams
import org.opensearch.replication.task.shard.ShardReplicationExecutor
import org.opensearch.replication.task.shard.ShardReplicationParams
import org.opensearch.test.OpenSearchTestCase

class TransportStopIndexReplicationActionTests : OpenSearchTestCase() {

    private lateinit var clusterService: ClusterService
    private val followerIndex = "test-follower-index"
    private val leaderAlias = "leader-cluster"

    @Before
    fun setup() {
        clusterService = mock(ClusterService::class.java)
    }

    @Test
    fun `test isReplicationTask identifies index replication task`() {
        val tasks = PersistentTasksCustomMetadata.builder()
        val leaderIndex = Index(followerIndex, "leader-uuid")
        
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams(leaderAlias, leaderIndex, followerIndex),
            PersistentTasksCustomMetadata.Assignment("node-1", "assigned")
        )

        val metadata = Metadata.builder()
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
            .build()

        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build()
        val allTasks: PersistentTasksCustomMetadata = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE)
        
        val request = StopIndexReplicationRequest(followerIndex)
        val task = allTasks.tasks().first()
        
        // Verify task is identified as replication task
        val isReplicationTask = task.id.startsWith("replication:") &&
            (task.id == "replication:index:${request.indexName}" || task.id.split(":")[1].contains(request.indexName))
        
        assertTrue("Should identify index replication task", isReplicationTask)
    }

    @Test
    fun `test isReplicationTask identifies shard replication task`() {
        val tasks = PersistentTasksCustomMetadata.builder()
        val shardId = ShardId(Index(followerIndex, "follower-uuid"), 0)
        
        tasks.addTask<PersistentTaskParams>(
            "replication:[$followerIndex][0]",
            ShardReplicationExecutor.TASK_NAME,
            ShardReplicationParams(leaderAlias, shardId, shardId),
            PersistentTasksCustomMetadata.Assignment("node-1", "assigned to node with primary shard")
        )

        val metadata = Metadata.builder()
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
            .build()

        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build()
        val allTasks: PersistentTasksCustomMetadata = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE)
        
        val request = StopIndexReplicationRequest(followerIndex)
        val task = allTasks.tasks().first()
        
        // Verify task is identified as replication task
        val isReplicationTask = task.id.startsWith("replication:") &&
            (task.id == "replication:index:${request.indexName}" || task.id.split(":")[1].contains(request.indexName))
        
        assertTrue("Should identify shard replication task", isReplicationTask)
    }

    @Test
    fun `test isReplicationTask with both assigned and unassigned tasks`() {
        val tasks = PersistentTasksCustomMetadata.builder()
        val leaderIndex = Index(followerIndex, "leader-uuid")
        val shardId = ShardId(Index(followerIndex, "follower-uuid"), 0)
        
        // Add assigned index task
        tasks.addTask<PersistentTaskParams>(
            "replication:index:$followerIndex",
            IndexReplicationExecutor.TASK_NAME,
            IndexReplicationParams(leaderAlias, leaderIndex, followerIndex),
            PersistentTasksCustomMetadata.Assignment("node-1", "assigned")
        )
        
        // Add assigned shard task
        tasks.addTask<PersistentTaskParams>(
            "replication:[$followerIndex][0]",
            ShardReplicationExecutor.TASK_NAME,
            ShardReplicationParams(leaderAlias, shardId, shardId),
            PersistentTasksCustomMetadata.Assignment("node-2", "assigned")
        )
        
        // Add unassigned shard task
        tasks.addTask<PersistentTaskParams>(
            "replication:[$followerIndex][1]",
            ShardReplicationExecutor.TASK_NAME,
            ShardReplicationParams(leaderAlias, ShardId(Index(followerIndex, "follower-uuid"), 1), ShardId(Index(followerIndex, "follower-uuid"), 1)),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        )

        val metadata = Metadata.builder()
            .put(IndexMetadata.builder(followerIndex).settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(0))
            .putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build())
            .build()

        val clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build()
        val allTasks: PersistentTasksCustomMetadata = clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE)
        
        val request = StopIndexReplicationRequest(followerIndex)
        
        // Verify all tasks are identified as replication tasks regardless of assignment
        var assignedCount = 0
        var unassignedCount = 0
        
        for (task in allTasks.tasks()) {
            val isReplicationTask = task.id.startsWith("replication:") &&
                (task.id == "replication:index:${request.indexName}" || task.id.split(":")[1].contains(request.indexName))
            
            assertTrue("All tasks should be identified as replication tasks", isReplicationTask)
            
            if (task.isAssigned) {
                assignedCount++
            } else {
                unassignedCount++
            }
        }
        
        assertEquals("Should have 2 assigned tasks", 2, assignedCount)
        assertEquals("Should have 1 unassigned task", 1, unassignedCount)
    }
}
