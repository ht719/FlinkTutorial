package com.atguigu.chapter09;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * @author hb30496
 * @description: CheckpointRestoreByIDEUtils
 * @date 2024-10-19
 */

public class CheckpointRestoreByIDEUtils {

    public static void run(
            @Nonnull StreamGraph streamGraph,
            @Nullable String externalCheckpoint) throws Exception {
        JobGraph jobGraph = streamGraph.getJobGraph();
        if (externalCheckpoint != null) {
            jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(externalCheckpoint));
        }

        int slotNum = getSlotNum(jobGraph);
        ClusterClient<?> clusterClient = initCluster(slotNum);
        clusterClient.submitJob(jobGraph).get();
    }

    private static int getSlotNum(JobGraph jobGraph) {
        HashMap<SlotSharingGroupId, Integer> map = new HashMap<>();
        for (JobVertex jobVertex : jobGraph.getVertices()) {
            SlotSharingGroupId slotSharingGroupId = jobVertex.getSlotSharingGroup()
                    .getSlotSharingGroupId();
            int parallelism = jobVertex.getParallelism();
            int oldParallelism = map.getOrDefault(slotSharingGroupId, 0);
            if (parallelism > oldParallelism) {
                map.put(slotSharingGroupId, parallelism);
            }
        }
        int slotNum = 0;
        for (int parallelism : map.values()) {
            slotNum += parallelism;
        }
        return slotNum;
    }

    private static ClusterClient<?> initCluster(int slotNum) throws Exception {
//        MiniClusterWithClientResource cluster = new MiniClusterWithClientResource(
//                new MiniClusterResourceConfiguration.Builder()
//                        .setNumberSlotsPerTaskManager(slotNum)
//                        .build());
//        cluster.before();
//        return cluster.getClusterClient();
        return null;
    }

}
