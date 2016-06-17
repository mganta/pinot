package com.linkedin.pinot.routing.builder;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.routing.ServerToSegmentSetMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Test for the Kafka low level consumer routing table builder.
 */
public class KafkaLowLevelConsumerRoutingTableBuilderTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLowLevelConsumerRoutingTableBuilderTest.class);

  @Test
  public void testRoutingTable() {
    final int ITERATIONS = 1000;
    Random random = new Random();

    KafkaLowLevelConsumerRoutingTableBuilder routingTableBuilder = new KafkaLowLevelConsumerRoutingTableBuilder();
    routingTableBuilder.init(null);

    long totalNanos = 0L;

    for (int i = 0; i < ITERATIONS; i++) {
      int instanceCount = random.nextInt(12) + 3; // 3 to 14 instances
      int partitionCount = random.nextInt(8) + 4; // 4 to 11 partitions

      // Generate instances
      String[] instanceNames = new String[instanceCount];
      for (int serverInstanceId = 0; serverInstanceId < instanceCount; serverInstanceId++) {
        instanceNames[serverInstanceId] = "Server_localhost_" + serverInstanceId;
      }

      // Generate partitions
      String[][] segmentNames = new String[partitionCount][];
      int totalSegmentCount = 0;
      for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
        int segmentCount = random.nextInt(32); // 0 to 31 partitions
        segmentNames[partitionId] = new String[segmentCount];
        for (int sequenceNumber = 0; sequenceNumber < segmentCount; sequenceNumber++) {
          segmentNames[partitionId][sequenceNumber] = SegmentNameBuilder.Realtime.buildLowLevelConsumerSegmentName(
              "table", Integer.toString(partitionId), Integer.toString(sequenceNumber), System.currentTimeMillis());
        }
        totalSegmentCount += segmentCount;
      }

      // Generate instance configurations
      List<InstanceConfig> instanceConfigs = new ArrayList<InstanceConfig>();
      for (String instanceName : instanceNames) {
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfigs.add(instanceConfig);
        instanceConfig.getRecord().setSimpleField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, "false");
      }

      // Generate a random external view
      ExternalView externalView = new ExternalView("table_REALTIME");
      int[] segmentCountForInstance = new int[instanceCount];
      int maxSegmentCountOnInstance = 0;
      for (int partitionId = 0; partitionId < segmentNames.length; partitionId++) {
        String[] segments = segmentNames[partitionId];

        // Assign each segment for this partition
        for (int segmentIndex = 0; segmentIndex < segments.length; segmentIndex++) {
          int instanceIndex = -1;
          int randomOffset = random.nextInt(instanceCount);

          // Pick the first random instance that has fewer than maxSegmentCountOnInstance segments assigned to it
          for (int j = 0; j < instanceCount; j++) {
            int potentialInstanceIndex = (j + randomOffset) % instanceCount;
            if (segmentCountForInstance[potentialInstanceIndex] < maxSegmentCountOnInstance) {
              instanceIndex = potentialInstanceIndex;
              break;
            }
          }

          // All replicas have exactly maxSegmentCountOnInstance, pick a replica and increment the max
          if (instanceIndex == -1) {
            maxSegmentCountOnInstance++;
            instanceIndex = randomOffset;
          }

          // Increment the segment count for the instance
          segmentCountForInstance[instanceIndex]++;

          // Add the segment to the external view
          // TODO Add some segments in CONSUMING state
          externalView.setState(segmentNames[partitionId][segmentIndex], instanceNames[instanceIndex], "ONLINE");
        }
      }

      // Create routing tables
      long startTime = System.nanoTime();
      List<ServerToSegmentSetMap> routingTables = routingTableBuilder.computeRoutingTableFromExternalView(
          "table_REALTIME", externalView, instanceConfigs);
      long endTime = System.nanoTime();
      totalNanos += endTime - startTime;

      // Check that all routing tables generated match all segments, with no duplicates
      for (ServerToSegmentSetMap routingTable : routingTables) {
        Set<String> assignedSegments = new HashSet<String>();

        for (String server : routingTable.getServerSet()) {
          for (String segment : routingTable.getSegmentSet(server)) {
            assertFalse(assignedSegments.contains(segment));
            assignedSegments.add(segment);
          }
        }

        assertEquals(assignedSegments.size(), totalSegmentCount);
      }
    }

    LOGGER.warn("Routing table building avg ms: " + totalNanos / (ITERATIONS * 1000000.0));
  }
}