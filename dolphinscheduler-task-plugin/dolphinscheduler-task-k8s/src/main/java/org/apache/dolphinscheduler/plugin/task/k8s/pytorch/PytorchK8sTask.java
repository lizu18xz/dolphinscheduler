/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.k8s.pytorch;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.CLUSTER;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.NAMESPACE_NAME;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.PYTORCH_K8S_OPERATOR;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sPytorchTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.Label;
import org.apache.dolphinscheduler.plugin.task.api.model.NodeSelectorExpression;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sPytorchTaskParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sTaskParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;

public class PytorchK8sTask extends AbstractK8sTask {

    /**
     * taskExecutionContext
     */
    private final TaskExecutionContext taskExecutionContext;

    /**
     * task parameters  页面参数，保持一致
     */
    private final K8sPytorchTaskParameters k8sTaskParameters;

    /**
     * @param taskRequest taskRequest
     */
    public PytorchK8sTask(TaskExecutionContext taskRequest) {
        super(taskRequest, PYTORCH_K8S_OPERATOR);
        this.taskExecutionContext = taskRequest;
        this.k8sTaskParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), K8sPytorchTaskParameters.class);
        log.info("Initialize pytorch k8s task parameters {}", JSONUtils.toPrettyJsonString(k8sTaskParameters));
        if (k8sTaskParameters == null || !k8sTaskParameters.checkParameters()) {
            throw new TaskException("PYTORCH K8S task params is not valid");
        }
    }

    @Override
    public List<String> getApplicationIds() throws TaskException {
        return Collections.emptyList();
    }

    @Override
    public AbstractParameters getParameters() {
        return k8sTaskParameters;
    }

    /**
     * 当前任务类型自定义参数
     */
    @Override
    protected String buildCommand() {
        K8sPytorchTaskMainParameters k8sPytorchTaskMainParameters = new K8sPytorchTaskMainParameters();
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();
        Map<String, String> namespace = JSONUtils.toMap(k8sTaskParameters.getNamespace());
        String namespaceName = namespace.get(NAMESPACE_NAME);
        String clusterName = namespace.get(CLUSTER);
        k8sPytorchTaskMainParameters.setImage(k8sTaskParameters.getImage());
        k8sPytorchTaskMainParameters.setNamespaceName(namespaceName);
        k8sPytorchTaskMainParameters.setClusterName(clusterName);
        k8sPytorchTaskMainParameters.setMasterMinMemorySpace(k8sTaskParameters.getMasterMinMemorySpace());
        k8sPytorchTaskMainParameters.setMasterMinCpuCores(k8sTaskParameters.getMasterMinCpuCores());
        k8sPytorchTaskMainParameters.setMasterGpuLimits(k8sTaskParameters.getMasterGpuLimits());
        k8sPytorchTaskMainParameters.setMasterReplicas(k8sTaskParameters.getMasterReplicas());
        k8sPytorchTaskMainParameters.setWorkerMinMemorySpace(k8sTaskParameters.getWorkerMinMemorySpace());
        k8sPytorchTaskMainParameters.setWorkerMinCpuCores(k8sTaskParameters.getWorkerMinCpuCores());
        k8sPytorchTaskMainParameters.setWorkerGpuLimits(k8sTaskParameters.getWorkerGpuLimits());
        k8sPytorchTaskMainParameters.setWorkerReplicas(k8sTaskParameters.getWorkerReplicas());
        k8sPytorchTaskMainParameters.setParamsMap(ParameterUtils.convert(paramsMap));
        k8sPytorchTaskMainParameters.setLabelMap(convertToLabelMap(k8sTaskParameters.getCustomizedLabels()));
        k8sPytorchTaskMainParameters
                .setNodeSelectorRequirements(convertToNodeSelectorRequirements(k8sTaskParameters.getNodeSelectors()));
        k8sPytorchTaskMainParameters.setCommand(k8sTaskParameters.getCommand());
        k8sPytorchTaskMainParameters.setArgs(k8sTaskParameters.getArgs());
        k8sPytorchTaskMainParameters.setImagePullPolicy(k8sTaskParameters.getImagePullPolicy());


        k8sPytorchTaskMainParameters.setQueue(k8sTaskParameters.getQueue());
        k8sPytorchTaskMainParameters.setGpuType(k8sTaskParameters.getGpuType());
        k8sPytorchTaskMainParameters.setOutputDataVolume(k8sTaskParameters.getOutputDataVolume());
        k8sPytorchTaskMainParameters.setInputDataVolume(k8sTaskParameters.getInputDataVolume());
        k8sPytorchTaskMainParameters.setPodInputDataVolume(k8sTaskParameters.getPodInputDataVolume());
        k8sPytorchTaskMainParameters.setPodOutputDataVolume(k8sTaskParameters.getPodOutputDataVolume());
        return JSONUtils.toJsonString(k8sPytorchTaskMainParameters);
    }

    public List<NodeSelectorRequirement> convertToNodeSelectorRequirements(List<NodeSelectorExpression> expressions) {
        if (CollectionUtils.isEmpty(expressions)) {
            return Collections.emptyList();
        }

        return expressions.stream().map(expression -> new NodeSelectorRequirement(
                        expression.getKey(),
                        expression.getOperator(),
                        StringUtils.isEmpty(expression.getValues()) ? Collections.emptyList()
                                : Arrays.asList(expression.getValues().trim().split("\\s*,\\s*"))))
                .collect(Collectors.toList());
    }

    public Map<String, String> convertToLabelMap(List<Label> labels) {
        if (CollectionUtils.isEmpty(labels)) {
            return Collections.emptyMap();
        }

        HashMap<String, String> labelMap = new HashMap<>();
        labels.forEach(label -> {
            labelMap.put(label.getLabel(), label.getValue());
        });
        return labelMap;
    }

}
