/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.dataset.remote.RemoteInferenceInputDataSet;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;
import org.opensearch.ml.common.utils.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(RCATool.TYPE)
public class RCATool implements Tool {

    public static final String TYPE = "RCATool";

    private String name = TYPE;

    private String description = DEFAULT_DESCRIPTION;

    private static final String MODEL_ID = "model_id";

    private final Client client;
    private final String modelId;

    private static final String DEFAULT_DESCRIPTION = "Use this tool to perform RCA analysis";

    public RCATool(Client client, String modelId) {
        this.client = client;
        this.modelId = modelId;
    }

    @Getter
    static class KnowledgeBase {
        public KnowledgeBase() {}

        @Getter
        static class Cause {
            public Cause() {}

            public String reason;
            public String API_URL;
            public String API_DOC;
            public String Example;
            @Setter
            public String response;
        }

        public String phenomenon;
        public List<KnowledgeBase.Cause> causes;
    }

    public static final String mocked_knowledge = "{\n"
        + "    \"phenomenon\": \"cluster or index health is red\",\n"
        + "    \"potential_causes\": [\n"
        + "        {\n"
        + "            \"reason\": \"Disk usage exceeds the high watermark cluster setting cluster.routing.allocation.disk.watermark.high which defaults to 90%, in this case, all shards of new created index will be unassigned, so the index is in red health status, so the cluster health is red\",\n"
        + "            \"API_URL\": \"_cluster/allocation/explain\",\n"
        + "            \"API_DOC\": \"---\\nlayout: default\\ntitle: Cluster allocation explain\\nnav_order: 10\\nparent: Cluster APIs\\nhas_children: false\\nredirect_from:\\n - /opensearch/rest-api/cluster-allocation/\\n---\\n\\n# Cluster allocation explain\\n**Introduced 1.0**\\n{: .label .label-purple }\\n\\nThe most basic cluster allocation explain request finds an unassigned shard and explains why it can't be allocated to a node.\\n\\nIf you add some options, you can instead get information on a specific shard, including why OpenSearch assigned it to its current node.\\n\\n\\n## Example\\n\\n```json\\nGET _cluster/allocation/explain?include_yes_decisions=true\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n```\\n{% include copy-curl.html %}\\n\\n## Path and HTTP methods\\n\\n```\\nGET _cluster/allocation/explain\\nPOST _cluster/allocation/explain\\n```\\n\\n\\n## URL parameters\\n\\nAll cluster allocation explain parameters are optional.\\n\\nParameter | Type | Description\\n:--- | :--- | :---\\ninclude_yes_decisions | Boolean | OpenSearch makes a series of yes or no decisions when trying to allocate a shard to a node. If this parameter is true, OpenSearch includes the (generally more numerous) \\\"yes\\\" decisions in its response. Default is false.\\ninclude_disk_info | Boolean | Whether to include information about disk usage in the response. Default is false.\\n\\n\\n## Request body\\n\\nAll cluster allocation explain fields are optional.\\n\\nField | Type | Description\\n:--- | :--- | :---\\ncurrent_node | String | If you only want an explanation if the shard happens to be on a particular node, specify that node name here.\\nindex | String | The name of the shard's index.\\nprimary | Boolean | Whether to provide an explanation for the primary shard (true) or its first replica (false), which share the same shard ID.\\nshard | Integer | The shard ID that you want an explanation for.\\n\\n\\n## Response\\n\\n```json\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"started\\\",\\n  \\\"current_node\\\": {\\n    \\\"id\\\": \\\"d8jRZcW1QmCBeVFlgOJx5A\\\",\\n    \\\"name\\\": \\\"opensearch-node1\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.4:9300\\\",\\n    \\\"weight_ranking\\\": 1\\n  },\\n  \\\"can_remain_on_current_node\\\": \\\"yes\\\",\\n  \\\"can_rebalance_cluster\\\": \\\"yes\\\",\\n  \\\"can_rebalance_to_other_node\\\": \\\"no\\\",\\n  \\\"rebalance_explanation\\\": \\\"cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance\\\",\\n  \\\"node_allocation_decisions\\\": [{\\n    \\\"node_id\\\": \\\"vRxi4uPcRt2BtHlFoyCyTQ\\\",\\n    \\\"node_name\\\": \\\"opensearch-node2\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.3:9300\\\",\\n    \\\"node_decision\\\": \\\"no\\\",\\n    \\\"weight_ranking\\\": 1,\\n    \\\"deciders\\\": [{\\n        \\\"decider\\\": \\\"max_retry\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard has no previous failures\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"replica_after_primary_active\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard is primary and can be allocated\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"enable\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"all allocations are allowed\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"node_version\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"can relocate primary shard from a node with version [1.0.0] to a node with equal-or-newer version [1.0.0]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"snapshot_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"no snapshots are currently running\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"restore_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"ignored as shard is not being recovered from a snapshot\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"filter\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"node passes include/exclude/require filters\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"same_shard\\\",\\n        \\\"decision\\\": \\\"NO\\\",\\n        \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[movies][0], node[vRxi4uPcRt2BtHlFoyCyTQ], [R], s[STARTED], a[id=x8w7QxWdQQa188HKGn0iMQ]]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"disk_threshold\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"enough disk for shard on node, free: [35.9gb], shard size: [15.1kb], free after allocating shard: [35.9gb]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"throttling\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"below shard recovery limit of outgoing: [0 < 2] incoming: [0 < 2]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"shards_limit\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"total shard limits are disabled: [index: -1, cluster: -1] <= 0\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"awareness\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"allocation awareness is not enabled, set cluster setting [cluster.routing.allocation.awareness.attributes] to enable it\\\"\\n      }\\n    ]\\n  }]\\n}\\n```\\n\",\n"
        + "            \"Example\": \"PUT _cluster/settings{ \\\"transient\\\": {\\\"cluster.routing.allocation.disk.watermark.low\\\": \\\"50%\\\",\\\"cluster.routing.allocation.disk.watermark.high\\\": \\\"51%\\\"}}.PUT test300\\n\\nGET _cluster/allocation/explain\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n\\nResponse of the allocation explain API:\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"unassigned\\\",\\n  \\\"unassigned_info\\\": {\\n    \\\"reason\\\": \\\"INDEX_CREATED\\\",\\n    \\\"at\\\": \\\"2024-07-29T03:10:02.770Z\\\",\\n    \\\"last_allocation_status\\\": \\\"no\\\"\\n  },\\n  \\\"can_allocate\\\": \\\"no\\\",\\n  \\\"allocate_explanation\\\": \\\"cannot allocate because allocation is not permitted to any of the nodes\\\",\\n  \\\"node_allocation_decisions\\\": [\\n    {\\n      \\\"node_id\\\": \\\"QUoa_v5KQDygjhzlPxh5fA\\\",\\n      \\\"node_name\\\": \\\"opensearch-node2\\\",\\n      \\\"transport_address\\\": \\\"172.19.0.3:9300\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"weight_ranking\\\": 1,\\n      \\\"deciders\\\": [\\n        {\\n          \\\"decider\\\": \\\"disk_threshold\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=51%], using more disk space than the maximum allowed [51.0%], actual free: [18.971559278120587%]\\\"\\n        }\\n      ]\\n    },\\n    {\\n      \\\"node_id\\\": \\\"ZBaTM_xGSbGi83TsLGpxxQ\\\",\\n      \\\"node_name\\\": \\\"opensearch-node1\\\",\\n      \\\"transport_address\\\": \\\"172.19.0.4:9300\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"weight_ranking\\\": 2,\\n      \\\"deciders\\\": [\\n        {\\n          \\\"decider\\\": \\\"disk_threshold\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=51%], using more disk space than the maximum allowed [51.0%], actual free: [18.971559278120587%]\\\"\\n        }\\n      ]\\n    }\\n  ]\\n}. Cannot allocate because allocation is not permitted to any of the nodes, the node is above the high watermark cluster setting [cluster.routing.allocation.disk.watermark.high=51%], using more disk space than the maximum allowed [51.0%], actual free: [18.971559278120587%]\"\n"
        + "        },\n"
        + "        {\n"
        + "            \"reason\": \"Node left, at least one index has no replicas, when one of the node which holds the shards of that index left, then the cluster health is red because some primary shards are unassigned\",\n"
        + "            \"API_URL\": \"_cluster/allocation/explain\",\n"
        + "            \"API_DOC\": \"---\\nlayout: default\\ntitle: Cluster allocation explain\\nnav_order: 10\\nparent: Cluster APIs\\nhas_children: false\\nredirect_from:\\n - /opensearch/rest-api/cluster-allocation/\\n---\\n\\n# Cluster allocation explain\\n**Introduced 1.0**\\n{: .label .label-purple }\\n\\nThe most basic cluster allocation explain request finds an unassigned shard and explains why it can't be allocated to a node.\\n\\nIf you add some options, you can instead get information on a specific shard, including why OpenSearch assigned it to its current node.\\n\\n\\n## Example\\n\\n```json\\nGET _cluster/allocation/explain?include_yes_decisions=true\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n```\\n{% include copy-curl.html %}\\n\\n## Path and HTTP methods\\n\\n```\\nGET _cluster/allocation/explain\\nPOST _cluster/allocation/explain\\n```\\n\\n\\n## URL parameters\\n\\nAll cluster allocation explain parameters are optional.\\n\\nParameter | Type | Description\\n:--- | :--- | :---\\ninclude_yes_decisions | Boolean | OpenSearch makes a series of yes or no decisions when trying to allocate a shard to a node. If this parameter is true, OpenSearch includes the (generally more numerous) \\\"yes\\\" decisions in its response. Default is false.\\ninclude_disk_info | Boolean | Whether to include information about disk usage in the response. Default is false.\\n\\n\\n## Request body\\n\\nAll cluster allocation explain fields are optional.\\n\\nField | Type | Description\\n:--- | :--- | :---\\ncurrent_node | String | If you only want an explanation if the shard happens to be on a particular node, specify that node name here.\\nindex | String | The name of the shard's index.\\nprimary | Boolean | Whether to provide an explanation for the primary shard (true) or its first replica (false), which share the same shard ID.\\nshard | Integer | The shard ID that you want an explanation for.\\n\\n\\n## Response\\n\\n```json\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"started\\\",\\n  \\\"current_node\\\": {\\n    \\\"id\\\": \\\"d8jRZcW1QmCBeVFlgOJx5A\\\",\\n    \\\"name\\\": \\\"opensearch-node1\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.4:9300\\\",\\n    \\\"weight_ranking\\\": 1\\n  },\\n  \\\"can_remain_on_current_node\\\": \\\"yes\\\",\\n  \\\"can_rebalance_cluster\\\": \\\"yes\\\",\\n  \\\"can_rebalance_to_other_node\\\": \\\"no\\\",\\n  \\\"rebalance_explanation\\\": \\\"cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance\\\",\\n  \\\"node_allocation_decisions\\\": [{\\n    \\\"node_id\\\": \\\"vRxi4uPcRt2BtHlFoyCyTQ\\\",\\n    \\\"node_name\\\": \\\"opensearch-node2\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.3:9300\\\",\\n    \\\"node_decision\\\": \\\"no\\\",\\n    \\\"weight_ranking\\\": 1,\\n    \\\"deciders\\\": [{\\n        \\\"decider\\\": \\\"max_retry\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard has no previous failures\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"replica_after_primary_active\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard is primary and can be allocated\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"enable\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"all allocations are allowed\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"node_version\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"can relocate primary shard from a node with version [1.0.0] to a node with equal-or-newer version [1.0.0]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"snapshot_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"no snapshots are currently running\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"restore_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"ignored as shard is not being recovered from a snapshot\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"filter\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"node passes include/exclude/require filters\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"same_shard\\\",\\n        \\\"decision\\\": \\\"NO\\\",\\n        \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[movies][0], node[vRxi4uPcRt2BtHlFoyCyTQ], [R], s[STARTED], a[id=x8w7QxWdQQa188HKGn0iMQ]]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"disk_threshold\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"enough disk for shard on node, free: [35.9gb], shard size: [15.1kb], free after allocating shard: [35.9gb]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"throttling\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"below shard recovery limit of outgoing: [0 < 2] incoming: [0 < 2]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"shards_limit\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"total shard limits are disabled: [index: -1, cluster: -1] <= 0\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"awareness\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"allocation awareness is not enabled, set cluster setting [cluster.routing.allocation.awareness.attributes] to enable it\\\"\\n      }\\n    ]\\n  }]\\n}\\n```\\n\",\n"
        + "            \"Example\": \"PUT test300 {\\\"settings\\\": {\\\"number_of_shards\\\": 10,\\\"number_of_replicas\\\": 0}}. Shutdown a node which holds the primary shards of the index test300. GET _cluster/allocation/explain\\n\\nResponse of the allocation explain API:\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"unassigned\\\",\\n  \\\"unassigned_info\\\": {\\n    \\\"reason\\\": \\\"NODE_LEFT\\\",\\n    \\\"at\\\": \\\"2024-07-29T03:43:42.712Z\\\",\\n    \\\"details\\\": \\\"node_left [tY9KoXsSQVuguZjwjVZ89g]\\\",\\n    \\\"last_allocation_status\\\": \\\"no_valid_shard_copy\\\"\\n  },\\n  \\\"can_allocate\\\": \\\"no_valid_shard_copy\\\",\\n  \\\"allocate_explanation\\\": \\\"cannot allocate because a previous copy of the primary shard existed but can no longer be found on the nodes in the cluster\\\",\\n  \\\"node_allocation_decisions\\\": [\\n    {\\n      \\\"node_id\\\": \\\"ocKZ6WTgQmG_PWLnESwnpg\\\",\\n      \\\"node_name\\\": \\\"bcd07466ee01\\\",\\n      \\\"transport_address\\\": \\\"127.0.0.1:9500\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"store\\\": {\\n        \\\"found\\\": false\\n      }\\n    }\\n  ]\\n}. Cannot allocate because a previous copy of the primary shard existed but can no longer be found on the nodes in the cluster.\"\n"
        + "        },\n"
        + "        {\n"
        + "            \"reason\": \"Index level or cluster level shard allocation filtering prevents the primary shards being assigned to any node\",\n"
        + "            \"API_URL\": \"_cluster/allocation/explain\",\n"
        + "            \"API_DOC\": \"---\\nlayout: default\\ntitle: Cluster allocation explain\\nnav_order: 10\\nparent: Cluster APIs\\nhas_children: false\\nredirect_from:\\n - /opensearch/rest-api/cluster-allocation/\\n---\\n\\n# Cluster allocation explain\\n**Introduced 1.0**\\n{: .label .label-purple }\\n\\nThe most basic cluster allocation explain request finds an unassigned shard and explains why it can't be allocated to a node.\\n\\nIf you add some options, you can instead get information on a specific shard, including why OpenSearch assigned it to its current node.\\n\\n\\n## Example\\n\\n```json\\nGET _cluster/allocation/explain?include_yes_decisions=true\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n```\\n{% include copy-curl.html %}\\n\\n## Path and HTTP methods\\n\\n```\\nGET _cluster/allocation/explain\\nPOST _cluster/allocation/explain\\n```\\n\\n\\n## URL parameters\\n\\nAll cluster allocation explain parameters are optional.\\n\\nParameter | Type | Description\\n:--- | :--- | :---\\ninclude_yes_decisions | Boolean | OpenSearch makes a series of yes or no decisions when trying to allocate a shard to a node. If this parameter is true, OpenSearch includes the (generally more numerous) \\\"yes\\\" decisions in its response. Default is false.\\ninclude_disk_info | Boolean | Whether to include information about disk usage in the response. Default is false.\\n\\n\\n## Request body\\n\\nAll cluster allocation explain fields are optional.\\n\\nField | Type | Description\\n:--- | :--- | :---\\ncurrent_node | String | If you only want an explanation if the shard happens to be on a particular node, specify that node name here.\\nindex | String | The name of the shard's index.\\nprimary | Boolean | Whether to provide an explanation for the primary shard (true) or its first replica (false), which share the same shard ID.\\nshard | Integer | The shard ID that you want an explanation for.\\n\\n\\n## Response\\n\\n```json\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"started\\\",\\n  \\\"current_node\\\": {\\n    \\\"id\\\": \\\"d8jRZcW1QmCBeVFlgOJx5A\\\",\\n    \\\"name\\\": \\\"opensearch-node1\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.4:9300\\\",\\n    \\\"weight_ranking\\\": 1\\n  },\\n  \\\"can_remain_on_current_node\\\": \\\"yes\\\",\\n  \\\"can_rebalance_cluster\\\": \\\"yes\\\",\\n  \\\"can_rebalance_to_other_node\\\": \\\"no\\\",\\n  \\\"rebalance_explanation\\\": \\\"cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance\\\",\\n  \\\"node_allocation_decisions\\\": [{\\n    \\\"node_id\\\": \\\"vRxi4uPcRt2BtHlFoyCyTQ\\\",\\n    \\\"node_name\\\": \\\"opensearch-node2\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.3:9300\\\",\\n    \\\"node_decision\\\": \\\"no\\\",\\n    \\\"weight_ranking\\\": 1,\\n    \\\"deciders\\\": [{\\n        \\\"decider\\\": \\\"max_retry\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard has no previous failures\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"replica_after_primary_active\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard is primary and can be allocated\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"enable\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"all allocations are allowed\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"node_version\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"can relocate primary shard from a node with version [1.0.0] to a node with equal-or-newer version [1.0.0]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"snapshot_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"no snapshots are currently running\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"restore_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"ignored as shard is not being recovered from a snapshot\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"filter\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"node passes include/exclude/require filters\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"same_shard\\\",\\n        \\\"decision\\\": \\\"NO\\\",\\n        \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[movies][0], node[vRxi4uPcRt2BtHlFoyCyTQ], [R], s[STARTED], a[id=x8w7QxWdQQa188HKGn0iMQ]]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"disk_threshold\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"enough disk for shard on node, free: [35.9gb], shard size: [15.1kb], free after allocating shard: [35.9gb]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"throttling\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"below shard recovery limit of outgoing: [0 < 2] incoming: [0 < 2]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"shards_limit\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"total shard limits are disabled: [index: -1, cluster: -1] <= 0\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"awareness\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"allocation awareness is not enabled, set cluster setting [cluster.routing.allocation.awareness.attributes] to enable it\\\"\\n      }\\n    ]\\n  }]\\n}\\n```\\n\",\n"
        + "            \"Example\": \"PUT test300{\\\"settings\\\": {\\\"index.routing.allocation.require.zone\\\": \\\"us-east-1\\\"}}. GET _cluster/allocation/explain\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": false\\n}\\n\\nResponse of the allocation explain API:\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": false,\\n  \\\"current_state\\\": \\\"unassigned\\\",\\n  \\\"unassigned_info\\\": {\\n    \\\"reason\\\": \\\"INDEX_CREATED\\\",\\n    \\\"at\\\": \\\"2024-07-29T04:51:46.703Z\\\",\\n    \\\"last_allocation_status\\\": \\\"no_attempt\\\"\\n  },\\n  \\\"can_allocate\\\": \\\"no\\\",\\n  \\\"allocate_explanation\\\": \\\"cannot allocate because allocation is not permitted to any of the nodes\\\",\\n  \\\"node_allocation_decisions\\\": [\\n    {\\n      \\\"node_id\\\": \\\"tY9KoXsSQVuguZjwjVZ89g\\\",\\n      \\\"node_name\\\": \\\"bcd07466ee01\\\",\\n      \\\"transport_address\\\": \\\"127.0.0.1:9300\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"weight_ranking\\\": 1,\\n      \\\"deciders\\\": [\\n        {\\n          \\\"decider\\\": \\\"same_shard\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[test300][0], node[tY9KoXsSQVuguZjwjVZ89g], [P], s[STARTED], a[id=4uGuBGSMQZ-HfR2rc63uLw]]\\\"\\n        }\\n      ]\\n    },\\n    {\\n      \\\"node_id\\\": \\\"ocKZ6WTgQmG_PWLnESwnpg\\\",\\n      \\\"node_name\\\": \\\"bcd07466ee01\\\",\\n      \\\"transport_address\\\": \\\"127.0.0.1:9500\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"weight_ranking\\\": 2,\\n      \\\"deciders\\\": [\\n        {\\n          \\\"decider\\\": \\\"same_shard\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[test300][0], node[ocKZ6WTgQmG_PWLnESwnpg], [R], s[STARTED], a[id=td6YyekFTu6F-w2M9GRA5g]]\\\"\\n        }\\n      ]\\n    }\\n  ]\\n}. Cannot allocate because allocation is not permitted to any of the nodes, node does not match index setting [index.routing.allocation.require] filters [zone:\\\"us-east-1\\\"]\"\n"
        + "        },\n"
        + "        {\n"
        + "            \"reason\": \"At least one primary shard is not assigned because the total shards in every node exceed the cluster level  cluster.routing.allocation.total_shards_per_node setting or the index level index.routing.allocation.total_shards_per_node setting\",\n"
        + "            \"API_URL\": \"_cluster/allocation/explain\",\n"
        + "            \"API_DOC\": \"---\\nlayout: default\\ntitle: Cluster allocation explain\\nnav_order: 10\\nparent: Cluster APIs\\nhas_children: false\\nredirect_from:\\n - /opensearch/rest-api/cluster-allocation/\\n---\\n\\n# Cluster allocation explain\\n**Introduced 1.0**\\n{: .label .label-purple }\\n\\nThe most basic cluster allocation explain request finds an unassigned shard and explains why it can't be allocated to a node.\\n\\nIf you add some options, you can instead get information on a specific shard, including why OpenSearch assigned it to its current node.\\n\\n\\n## Example\\n\\n```json\\nGET _cluster/allocation/explain?include_yes_decisions=true\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n```\\n{% include copy-curl.html %}\\n\\n## Path and HTTP methods\\n\\n```\\nGET _cluster/allocation/explain\\nPOST _cluster/allocation/explain\\n```\\n\\n\\n## URL parameters\\n\\nAll cluster allocation explain parameters are optional.\\n\\nParameter | Type | Description\\n:--- | :--- | :---\\ninclude_yes_decisions | Boolean | OpenSearch makes a series of yes or no decisions when trying to allocate a shard to a node. If this parameter is true, OpenSearch includes the (generally more numerous) \\\"yes\\\" decisions in its response. Default is false.\\ninclude_disk_info | Boolean | Whether to include information about disk usage in the response. Default is false.\\n\\n\\n## Request body\\n\\nAll cluster allocation explain fields are optional.\\n\\nField | Type | Description\\n:--- | :--- | :---\\ncurrent_node | String | If you only want an explanation if the shard happens to be on a particular node, specify that node name here.\\nindex | String | The name of the shard's index.\\nprimary | Boolean | Whether to provide an explanation for the primary shard (true) or its first replica (false), which share the same shard ID.\\nshard | Integer | The shard ID that you want an explanation for.\\n\\n\\n## Response\\n\\n```json\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"started\\\",\\n  \\\"current_node\\\": {\\n    \\\"id\\\": \\\"d8jRZcW1QmCBeVFlgOJx5A\\\",\\n    \\\"name\\\": \\\"opensearch-node1\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.4:9300\\\",\\n    \\\"weight_ranking\\\": 1\\n  },\\n  \\\"can_remain_on_current_node\\\": \\\"yes\\\",\\n  \\\"can_rebalance_cluster\\\": \\\"yes\\\",\\n  \\\"can_rebalance_to_other_node\\\": \\\"no\\\",\\n  \\\"rebalance_explanation\\\": \\\"cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance\\\",\\n  \\\"node_allocation_decisions\\\": [{\\n    \\\"node_id\\\": \\\"vRxi4uPcRt2BtHlFoyCyTQ\\\",\\n    \\\"node_name\\\": \\\"opensearch-node2\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.3:9300\\\",\\n    \\\"node_decision\\\": \\\"no\\\",\\n    \\\"weight_ranking\\\": 1,\\n    \\\"deciders\\\": [{\\n        \\\"decider\\\": \\\"max_retry\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard has no previous failures\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"replica_after_primary_active\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard is primary and can be allocated\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"enable\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"all allocations are allowed\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"node_version\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"can relocate primary shard from a node with version [1.0.0] to a node with equal-or-newer version [1.0.0]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"snapshot_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"no snapshots are currently running\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"restore_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"ignored as shard is not being recovered from a snapshot\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"filter\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"node passes include/exclude/require filters\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"same_shard\\\",\\n        \\\"decision\\\": \\\"NO\\\",\\n        \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[movies][0], node[vRxi4uPcRt2BtHlFoyCyTQ], [R], s[STARTED], a[id=x8w7QxWdQQa188HKGn0iMQ]]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"disk_threshold\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"enough disk for shard on node, free: [35.9gb], shard size: [15.1kb], free after allocating shard: [35.9gb]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"throttling\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"below shard recovery limit of outgoing: [0 < 2] incoming: [0 < 2]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"shards_limit\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"total shard limits are disabled: [index: -1, cluster: -1] <= 0\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"awareness\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"allocation awareness is not enabled, set cluster setting [cluster.routing.allocation.awareness.attributes] to enable it\\\"\\n      }\\n    ]\\n  }]\\n}\\n```\\n\",\n"
        + "            \"Example\": \"PUT _cluster/settings{\\\"transient\\\": {\\\"cluster.routing.allocation.total_shards_per_node\\\":2}} PUT test300{\\\"settings\\\": {\\\"index.number_of_shards\\\": 10}}. GET _cluster/allocation/explain\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n\\nResponse of the allocation explain API:\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": false,\\n  \\\"current_state\\\": \\\"unassigned\\\",\\n  \\\"unassigned_info\\\": {\\n    \\\"reason\\\": \\\"INDEX_CREATED\\\",\\n    \\\"at\\\": \\\"2024-07-29T04:54:06.274Z\\\",\\n    \\\"last_allocation_status\\\": \\\"no_attempt\\\"\\n  },\\n  \\\"can_allocate\\\": \\\"no\\\",\\n  \\\"allocate_explanation\\\": \\\"cannot allocate because allocation is not permitted to any of the nodes\\\",\\n  \\\"node_allocation_decisions\\\": [\\n    {\\n      \\\"node_id\\\": \\\"tY9KoXsSQVuguZjwjVZ89g\\\",\\n      \\\"node_name\\\": \\\"bcd07466ee01\\\",\\n      \\\"transport_address\\\": \\\"127.0.0.1:9300\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"weight_ranking\\\": 1,\\n      \\\"deciders\\\": [\\n        {\\n          \\\"decider\\\": \\\"same_shard\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[test300][0], node[tY9KoXsSQVuguZjwjVZ89g], [P], s[STARTED], a[id=fHyJcT2cTj6MKbpuvd0KHQ]]\\\"\\n        },\\n        {\\n          \\\"decider\\\": \\\"shards_limit\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"too many shards [2] allocated to this node for index [test300], index setting [index.routing.allocation.total_shards_per_node=2]\\\"\\n        }\\n      ]\\n    },\\n    {\\n      \\\"node_id\\\": \\\"ocKZ6WTgQmG_PWLnESwnpg\\\",\\n      \\\"node_name\\\": \\\"bcd07466ee01\\\",\\n      \\\"transport_address\\\": \\\"127.0.0.1:9500\\\",\\n      \\\"node_attributes\\\": {\\n        \\\"shard_indexing_pressure_enabled\\\": \\\"true\\\"\\n      },\\n      \\\"node_decision\\\": \\\"no\\\",\\n      \\\"weight_ranking\\\": 2,\\n      \\\"deciders\\\": [\\n        {\\n          \\\"decider\\\": \\\"shards_limit\\\",\\n          \\\"decision\\\": \\\"NO\\\",\\n          \\\"explanation\\\": \\\"too many shards [2] allocated to this node for index [test300], index setting [index.routing.allocation.total_shards_per_node=2]\\\"\\n        }\\n      ]\\n    }\\n  ]\\n}. Cannot allocate because allocation is not permitted to any of the nodes,too many shards [2] allocated to this node, cluster setting [cluster.routing.allocation.total_shards_per_node=2]\"\n"
        + "        },\n"
        + "        {\n"
        + "            \"reason\": \"The segments file of a primary shard is corrupt, and there’re no available replicas\",\n"
        + "            \"API_URL\": \"_cluster/allocation/explain\",\n"
        + "            \"API_DOC\": \"---\\nlayout: default\\ntitle: Cluster allocation explain\\nnav_order: 10\\nparent: Cluster APIs\\nhas_children: false\\nredirect_from:\\n - /opensearch/rest-api/cluster-allocation/\\n---\\n\\n# Cluster allocation explain\\n**Introduced 1.0**\\n{: .label .label-purple }\\n\\nThe most basic cluster allocation explain request finds an unassigned shard and explains why it can't be allocated to a node.\\n\\nIf you add some options, you can instead get information on a specific shard, including why OpenSearch assigned it to its current node.\\n\\n\\n## Example\\n\\n```json\\nGET _cluster/allocation/explain?include_yes_decisions=true\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true\\n}\\n```\\n{% include copy-curl.html %}\\n\\n## Path and HTTP methods\\n\\n```\\nGET _cluster/allocation/explain\\nPOST _cluster/allocation/explain\\n```\\n\\n\\n## URL parameters\\n\\nAll cluster allocation explain parameters are optional.\\n\\nParameter | Type | Description\\n:--- | :--- | :---\\ninclude_yes_decisions | Boolean | OpenSearch makes a series of yes or no decisions when trying to allocate a shard to a node. If this parameter is true, OpenSearch includes the (generally more numerous) \\\"yes\\\" decisions in its response. Default is false.\\ninclude_disk_info | Boolean | Whether to include information about disk usage in the response. Default is false.\\n\\n\\n## Request body\\n\\nAll cluster allocation explain fields are optional.\\n\\nField | Type | Description\\n:--- | :--- | :---\\ncurrent_node | String | If you only want an explanation if the shard happens to be on a particular node, specify that node name here.\\nindex | String | The name of the shard's index.\\nprimary | Boolean | Whether to provide an explanation for the primary shard (true) or its first replica (false), which share the same shard ID.\\nshard | Integer | The shard ID that you want an explanation for.\\n\\n\\n## Response\\n\\n```json\\n{\\n  \\\"index\\\": \\\"movies\\\",\\n  \\\"shard\\\": 0,\\n  \\\"primary\\\": true,\\n  \\\"current_state\\\": \\\"started\\\",\\n  \\\"current_node\\\": {\\n    \\\"id\\\": \\\"d8jRZcW1QmCBeVFlgOJx5A\\\",\\n    \\\"name\\\": \\\"opensearch-node1\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.4:9300\\\",\\n    \\\"weight_ranking\\\": 1\\n  },\\n  \\\"can_remain_on_current_node\\\": \\\"yes\\\",\\n  \\\"can_rebalance_cluster\\\": \\\"yes\\\",\\n  \\\"can_rebalance_to_other_node\\\": \\\"no\\\",\\n  \\\"rebalance_explanation\\\": \\\"cannot rebalance as no target node exists that can both allocate this shard and improve the cluster balance\\\",\\n  \\\"node_allocation_decisions\\\": [{\\n    \\\"node_id\\\": \\\"vRxi4uPcRt2BtHlFoyCyTQ\\\",\\n    \\\"node_name\\\": \\\"opensearch-node2\\\",\\n    \\\"transport_address\\\": \\\"172.24.0.3:9300\\\",\\n    \\\"node_decision\\\": \\\"no\\\",\\n    \\\"weight_ranking\\\": 1,\\n    \\\"deciders\\\": [{\\n        \\\"decider\\\": \\\"max_retry\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard has no previous failures\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"replica_after_primary_active\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"shard is primary and can be allocated\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"enable\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"all allocations are allowed\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"node_version\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"can relocate primary shard from a node with version [1.0.0] to a node with equal-or-newer version [1.0.0]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"snapshot_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"no snapshots are currently running\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"restore_in_progress\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"ignored as shard is not being recovered from a snapshot\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"filter\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"node passes include/exclude/require filters\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"same_shard\\\",\\n        \\\"decision\\\": \\\"NO\\\",\\n        \\\"explanation\\\": \\\"a copy of this shard is already allocated to this node [[movies][0], node[vRxi4uPcRt2BtHlFoyCyTQ], [R], s[STARTED], a[id=x8w7QxWdQQa188HKGn0iMQ]]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"disk_threshold\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"enough disk for shard on node, free: [35.9gb], shard size: [15.1kb], free after allocating shard: [35.9gb]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"throttling\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"below shard recovery limit of outgoing: [0 < 2] incoming: [0 < 2]\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"shards_limit\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"total shard limits are disabled: [index: -1, cluster: -1] <= 0\\\"\\n      },\\n      {\\n        \\\"decider\\\": \\\"awareness\\\",\\n        \\\"decision\\\": \\\"YES\\\",\\n        \\\"explanation\\\": \\\"allocation awareness is not enabled, set cluster setting [cluster.routing.allocation.awareness.attributes] to enable it\\\"\\n      }\\n    ]\\n  }]\\n}\\n```\\n\",\n"
        + "            \"Example\": \"GET _cluster/allocation/explain\\n{\\n  \\\"index\\\": \\\"test300\\\",\\n  \\\"shard\\\": 9,\\n  \\\"primary\\\": true\\n}\\n\\nThe response of the allocation explain API just like:\\n{\\n  \\\"index\\\" : \\\"check\\\",\\n  \\\"shard\\\" : 0,\\n  \\\"primary\\\" : true,\\n  \\\"current_state\\\" : \\\"unassigned\\\",\\n  \\\"unassigned_info\\\" : {\\n    \\\"reason\\\" : \\\"INDEX_REOPENED\\\",\\n    \\\"at\\\" : \\\"2019-12-27T10:54:47.409Z\\\",\\n    \\\"last_allocation_status\\\" : \\\"no_valid_shard_copy\\\"\\n  },\\n  \\\"can_allocate\\\" : \\\"no_valid_shard_copy\\\",\\n  \\\"allocate_explanation\\\" : \\\"cannot allocate because all found copies of the shard are either stale or corrupt\\\",\\n  \\\"node_allocation_decisions\\\" : [\\n    {\\n      \\\"node_id\\\" : \\\"FRxaEjtqSROTMnAVdCs4lg\\\",\\n      \\\"node_name\\\" : \\\"FRxaEjt\\\",\\n      \\\"transport_address\\\" : \\\"<ip_address>:9300\\\",\\n      \\\"node_decision\\\" : \\\"no\\\",\\n      \\\"store\\\" : {\\n        \\\"in_sync\\\" : true,\\n        \\\"allocation_id\\\" : \\\"_4cYlrP4R6ehcW08lOma9g\\\",\\n        \\\"store_exception\\\" : {\\n          \\\"type\\\" : \\\"corrupt_index_exception\\\",\\n          \\\"reason\\\" : \\\"failed engine (reason: [corrupt file (source: [start])]) (resource=preexisting_corruption)\\\",\\n          \\\"caused_by\\\" : {\\n            \\\"type\\\" : \\\"i_o_exception\\\",\\n            \\\"reason\\\" : \\\"failed engine (reason: [corrupt file (source: [start])])\\\",\\n            \\\"caused_by\\\" : {\\n              \\\"type\\\" : \\\"corrupt_index_exception\\\",\\n              \\\"reason\\\" : \\\"codec footer mismatch (file truncated?): actual footer=139953464 vs expected footer=-1071082520 (resource=MMapIndexInput(path=\\\\\\\"/data/nodes/0/indices/EUWYntUMQEOBPeduvbhGgw/0/index/_fe_Lucene50_0.tim\\\\\\\"))\\\"\\n            }\\n          }\\n        }\\n      }\\n    }\\n  ]\\n}. Cannot allocate because all found copies of the shard are either stale or corrupt,failed engine (reason: [corrupt file (source: [start])]) (resource=preexisting_corruption),failed engine (reason: [corrupt file (source: [start])]),codec footer mismatch (file truncated?): actual footer=139953464 vs expected footer=-1071082520 (resource=MMapIndexInput(path=\\\"/data/nodes/0/indices/EUWYntUMQEOBPeduvbhGgw/0/index/_fe_Lucene50_0.tim\\\"))\"\n"
        + "        }\n"
        + "    ]\n"
        + "}";

    public static final String TOOL_PROMPT =
        "You are going to help find the root cause of the phenomenon from the several potential causes listed below. In this RCA process, for each cause, it usually needs to call an API to get some necessary information verify whether it's the right root cause. I've filled the related response for each cause, you should decide which cause are most possible to be the root cause based on these responses. \n\n"
            + "Human: PHENOMENON\n"
            + "--------------------\n"
            + "${parameters.phenomenon} \n\n"
            + "Human: POTENTIAL CAUSES AND RESPONSE\n"
            + "--------------------\n"
            + "${parameters.causes}";

    @Override
    @SuppressWarnings("unchecked")

    /**
     *
     * @param parameters contains parameters:
     *                   1. index
     *                   2. KNOWLEDGE
     * @param listener
     * @param <T>
     */
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        // KnowledgeBase knowledgeBase = StringUtils.gson.fromJson(knowledge, KnowledgeBase.class);
        String knowledge = parameters.getOrDefault("knowledge", mocked_knowledge);
        Map<String, ?> knowledgeBase = StringUtils.gson.fromJson(knowledge, Map.class);
        List<Map<String, String>> causes = (List<Map<String, String>>) knowledgeBase.get("potential_causes");
        Map<String, String> apiToResponse = causes
            .stream()
            .map(c -> c.get("API_URL"))
            .distinct()
            .collect(Collectors.toMap(url -> url, url -> invokeAPI(url, parameters)));
        causes.forEach(cause -> cause.put("response", apiToResponse.get(cause.get("API_URL"))));
        Map<String, String> LLMParams = new java.util.HashMap<>(
            Map.of("phenomenon", (String) knowledgeBase.get("phenomenon"), "causes", StringUtils.gson.toJson(causes))
        );
        StringSubstitutor substitute = new StringSubstitutor(LLMParams, "${parameters.", "}");
        String finalToolPrompt = substitute.replace(TOOL_PROMPT);
        log.error("qh finalToolPrompt: " + finalToolPrompt);
        LLMParams.put("prompt", finalToolPrompt);
        RemoteInferenceInputDataSet inputDataSet = RemoteInferenceInputDataSet.builder().parameters(LLMParams).build();
        ActionRequest request = new MLPredictionTaskRequest(
            modelId,
            MLInput.builder().algorithm(FunctionName.REMOTE).inputDataset(inputDataSet).build()
        );
        client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(r -> {
            ModelTensorOutput modelTensorOutput = (ModelTensorOutput) r.getOutput();
            Map<String, ?> dataMap = Optional
                .ofNullable(modelTensorOutput.getMlModelOutputs())
                .flatMap(outputs -> outputs.stream().findFirst())
                .flatMap(modelTensors -> modelTensors.getMlModelTensors().stream().findFirst())
                .map(ModelTensor::getDataAsMap)
                .orElse(null);
            if (dataMap == null) {
                throw new IllegalArgumentException("No dataMap returned from LLM.");
            }
            listener.onResponse((T) dataMap.get("response"));
        }, listener::onFailure));
    }

    private String invokeAPI(String url, Map<String, String> parameters) {
        switch (url) {
            case "_cluster/allocation/explain":
                ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest();
                request.setIndex(parameters.get("index"));
                request.setPrimary(true);
                request.setShard(0);
                try {
                    // TODO: need to be optimized to use listener to avoid block wait
                    ClusterAllocationExplanation clusterAllocationExplanation = client
                        .admin()
                        .cluster()
                        .allocationExplain(request)
                        .get()
                        .getExplanation();
                    XContentBuilder xContentBuilder = clusterAllocationExplanation
                        .toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
                    return xContentBuilder.toString();
                } catch (Exception e) {
                    return "";
                }

            default:
                return "";
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getVersion() {
        return null;
    }

    @Override
    public boolean validate(Map<String, String> parameters) {
        return parameters != null;
    }

    public static class Factory implements Tool.Factory<RCATool> {
        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (RCATool.class) {
                if (INSTANCE != null) {
                    return INSTANCE;
                }
                INSTANCE = new Factory();
                return INSTANCE;
            }
        }

        public void init(Client client) {
            this.client = client;
        }

        @Override
        public RCATool create(Map<String, Object> parameters) {
            String modelId = (String) parameters.get(MODEL_ID);
            if (Strings.isBlank(modelId)) {
                throw new IllegalArgumentException("model_id cannot be null or blank.");
            }
            return new RCATool(client, modelId);
        }

        @Override
        public String getDefaultDescription() {
            return DEFAULT_DESCRIPTION;
        }

        @Override
        public String getDefaultType() {
            return TYPE;
        }

        @Override
        public String getDefaultVersion() {
            return null;
        }
    }
}
