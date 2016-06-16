/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.routing.builder;

import com.linkedin.pinot.routing.ServerToSegmentSetMap;
import java.util.List;
import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;


/**
 * Routing table builder for the Kafka low level consumer.
 */
public class KafkaLowLevelConsumerRoutingTableBuilder implements RoutingTableBuilder {
  @Override
  public void init(Configuration configuration) {
    // No configuration at the moment
  }

  @Override
  public List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList) {
    // We build the routing table based off the external view here. What we want to do is to make sure that we uphold
    // the guarantees clients expect (no duplicate records, eventual consistency) and spreading the load as equally as
    // possible between the servers.
    //
    // Each Kafka partition contains a fraction of the data, so we need to make sure that we query all partitions.
    // Because in certain unlikely degenerate scenarios, we can consume overlapping data until segments are flushed (at
    // which point the overlapping data is discarded during the reconciliation process with the controller), we need to
    // ensure that the query that is sent has only one partition in CONSUMING state in order to avoid duplicate records.
    //
    // Because we also want to want to spread the load as equally as possible between servers, we use a weighted random
    // replica selection that favors picking replicas with fewer segments assigned to them, thus having an approximately
    // equal distribution of load between servers.
    //
    // For example, given three replicas with 1, 2 and 3 segments assigned to each, the replica with one segment should
    // have a weight of 2, which is the maximum segment count minus the segment count for that replica. Thus, each
    // replica other than the replica(s) with the maximum segment count should have a chance of getting a segment
    // assigned to it. This corresponds to alternative three below:
    //
    // Alternative 1 (weight is sum of segment counts - segment count in that replica):
    // (6 - 1) = 5 -> P(0.4166)
    // (6 - 2) = 4 -> P(0.3333)
    // (6 - 3) = 3 -> P(0.2500)
    //
    // Alternative 2 (weight is max of segment counts - segment count in that replica + 1):
    // (3 - 1) + 1 = 3 -> P(0.5000)
    // (3 - 2) + 1 = 2 -> P(0.3333)
    // (3 - 3) + 1 = 1 -> P(0.1666)
    //
    // Alternative 3 (weight is max of segment counts - segment count in that replica):
    // (3 - 1) = 2 -> P(0.6666)
    // (3 - 2) = 1 -> P(0.3333)
    // (3 - 3) = 0 -> P(0.0000)
    //
    // Of those three weighting alternatives, the third one has the smallest standard deviation of the number of
    // segments assigned per replica, so it corresponds to the weighting strategy used for segment assignment. Empirical
    // testing shows that for 20 segments and three replicas, the standard deviation of each alternative is respectively
    // 2.112, 1.496 and 0.853.
    //
    // This algorithm works as follows:
    // 1. Gather all segments and group them by Kafka partition
    // 2. Ensure that for each partition, we have at most one partition in consuming state
    // 3. Sort all the segments to be used during assignment in ascending order of replicas
    // 4. For each segment to be used during assignment, pick a random replica, weighted by the number of segments
    //    assigned to each replica.

    // 1. Gather all segments and group them by Kafka partition

    // 2. Ensure that for each partition, we have at most one partition in consuming state

    // 3. Sort all the segments to be used during assignment in ascending order of replicas

    // 4. For each segment to be used during assignment, pick a random replica, weighted by the number of segments
    //    assigned to each replica.

    return null;
  }
}
