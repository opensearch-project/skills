/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools;

import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.ActionRequest;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ml.common.FunctionName;
import org.opensearch.ml.common.input.MLInput;
import org.opensearch.ml.common.output.model.ModelTensor;
import org.opensearch.ml.common.output.model.ModelTensorOutput;
import org.opensearch.ml.common.output.model.ModelTensors;
import org.opensearch.ml.common.spi.tools.Tool;
import org.opensearch.ml.common.spi.tools.ToolAnnotation;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskAction;
import org.opensearch.ml.common.transport.prediction.MLPredictionTaskRequest;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Setter
@Getter
@ToolAnnotation(PPLTool.TYPE)
public class KnowledgeBaseTool implements Tool {

    public static final String TYPE = "KnowledgeBaseTool";

    private String name = TYPE;

    private String description = DEFAULT_DESCRIPTION;

    private String modelId;

    public static final String INPUT_FIELD = "query";

    private Client client;

    private static final String DEFAULT_DESCRIPTION = "Knowledge base tool description";

    public KnowledgeBaseTool(Client client, String modelId) {
        this.client = client;
        this.modelId = modelId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void run(Map<String, String> parameters, ActionListener<T> listener) {
        ActionRequest request = new MLPredictionTaskRequest(modelId, MLInput.builder().algorithm(FunctionName.REMOTE).build(), null);

        client.execute(MLPredictionTaskAction.INSTANCE, request, ActionListener.wrap(mlTaskResponse -> {
            ModelTensorOutput modelTensorOutput = (ModelTensorOutput) mlTaskResponse.getOutput();
            ModelTensors modelTensors = modelTensorOutput.getMlModelOutputs().get(0);
            ModelTensor modelTensor = modelTensors.getMlModelTensors().get(0);
            Map<String, String> dataAsMap = (Map<String, String>) modelTensor.getDataAsMap();
            if (dataAsMap.get("response") == null) {
                listener.onFailure(new IllegalStateException("Remote endpoint fails to retrieve documents"));
                return;
            }
            String retrievedDocuments = dataAsMap.get("response");
            listener.onResponse((T) retrievedDocuments);
        }, e -> {
            log.error(String.format(Locale.ROOT, "fail to predict model: %s with error: %s", modelId, e.getMessage()), e);
            listener.onFailure(e);
        }));
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

    public static class Factory implements Tool.Factory<KnowledgeBaseTool> {
        private Client client;

        private static Factory INSTANCE;

        public static Factory getInstance() {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            synchronized (PPLTool.class) {
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
        public KnowledgeBaseTool create(Map<String, Object> parameters) {
            validateKnowledgeBaseToolParameters(parameters);
            return new KnowledgeBaseTool(client, (String) parameters.get("model_id"));
        }

        private static void validateKnowledgeBaseToolParameters(Map<String, Object> parameters) {
            if (StringUtils.isBlank((String) parameters.get("model_id"))) {
                throw new IllegalArgumentException("Knowledge Base tool needs non blank model id.");
            }
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
