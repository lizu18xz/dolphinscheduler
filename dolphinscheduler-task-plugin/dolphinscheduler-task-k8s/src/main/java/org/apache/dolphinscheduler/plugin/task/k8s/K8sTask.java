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

package org.apache.dolphinscheduler.plugin.task.k8s;

import static org.apache.dolphinscheduler.common.constants.Constants.K8S_VOLUME;
import static org.apache.dolphinscheduler.common.constants.Constants.PRE_NODE_OUTPUT;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.CLUSTER;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.NAMESPACE_NAME;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.DataType;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.Label;
import org.apache.dolphinscheduler.plugin.task.api.model.NodeSelectorExpression;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
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

public class K8sTask extends AbstractK8sTask {

    /**
     * taskExecutionContext
     */
    private final TaskExecutionContext taskExecutionContext;

    /**
     * task parameters
     */
    private final K8sTaskParameters k8sTaskParameters;

    /**
     * @param taskRequest taskRequest
     */
    public K8sTask(TaskExecutionContext taskRequest) {
        super(taskRequest);
        this.taskExecutionContext = taskRequest;
        this.k8sTaskParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), K8sTaskParameters.class);
        log.info("Initialize k8s task parameters {}", JSONUtils.toPrettyJsonString(k8sTaskParameters));
        if (k8sTaskParameters == null || !k8sTaskParameters.checkParameters()) {
            throw new TaskException("K8S task params is not valid");
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

    @Override
    protected String buildCommand() {
        K8sTaskMainParameters k8sTaskMainParameters = new K8sTaskMainParameters();
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();
        String globalParams = taskExecutionContext.getGlobalParams();
        log.info("paramsMap:{}", JSONUtils.toJsonString(paramsMap));
        log.info("globalParams:{}", JSONUtils.toJsonString(globalParams));
        //获取自定义的参数，替换
        Map<String, String> paramMap = ParameterUtils.convert(paramsMap);
        //[\"/data\",\"asdad---123\"]",
        String k8sPodArgs = paramMap.get("k8s_pod_args");
        if (!StringUtils.isEmpty(k8sPodArgs)) {
            //替换参数为自定义接口传参
            k8sTaskParameters.setArgs(k8sPodArgs);
        }
        Map<String, String> namespace = JSONUtils.toMap(k8sTaskParameters.getNamespace());
        String namespaceName = namespace.get(NAMESPACE_NAME);
        String clusterName = namespace.get(CLUSTER);
        k8sTaskMainParameters.setImage(k8sTaskParameters.getImage());
        k8sTaskMainParameters.setNamespaceName(namespaceName);
        k8sTaskMainParameters.setClusterName(clusterName);
        k8sTaskMainParameters.setMinCpuCores(k8sTaskParameters.getMinCpuCores());
        k8sTaskMainParameters.setMinMemorySpace(k8sTaskParameters.getMinMemorySpace());
        k8sTaskMainParameters.setParamsMap(ParameterUtils.convert(paramsMap));
        k8sTaskMainParameters.setLabelMap(convertToLabelMap(k8sTaskParameters.getCustomizedLabels()));
        k8sTaskMainParameters
                .setNodeSelectorRequirements(convertToNodeSelectorRequirements(k8sTaskParameters.getNodeSelectors()));
        k8sTaskMainParameters.setCommand(k8sTaskParameters.getCommand());
        k8sTaskMainParameters.setArgs(k8sTaskParameters.getArgs());
        k8sTaskMainParameters.setImagePullPolicy(k8sTaskParameters.getImagePullPolicy());

        //直接约定死
        String volumePrefix = PropertyUtils.getString(K8S_VOLUME) + "/" + taskExecutionContext.getProjectCode();

        String inputDataVolume = volumePrefix + "/fetch/";
        if (!StringUtils.isEmpty(k8sTaskParameters.getFetchId())) {
            k8sTaskMainParameters.setFetchType(k8sTaskParameters.getFetchType());
            k8sTaskMainParameters.setFetchDataVolume(volumePrefix + "/fetch/");
            k8sTaskMainParameters.setFetchDataVolumeArgs(k8sTaskParameters.getFetchDataVolumeArgs());
        } else {
            //设置前置节点的输出作为输入 /mnt/k8s_volume/14912393717952/output/455/0
            String preTaskInstanceId = paramMap.get(PRE_NODE_OUTPUT);
            inputDataVolume = volumePrefix + "/output/" + preTaskInstanceId;
            log.info("pre task output set now task:{}", inputDataVolume);
        }

        //设置约定的挂载信息
        k8sTaskMainParameters.setOutputDataVolume(volumePrefix + "/output/");
        k8sTaskMainParameters.setInputDataVolume(inputDataVolume);
        k8sTaskMainParameters.setPodInputDataVolume("/data/input");
        k8sTaskMainParameters.setPodOutputDataVolume("/data/output");

        k8sTaskMainParameters.setGpuType(k8sTaskParameters.getGpuType());
        k8sTaskMainParameters.setGpuLimits(k8sTaskParameters.getGpuLimits());
        k8sTaskMainParameters.setQueue(k8sTaskParameters.getQueue());

        return JSONUtils.toJsonString(k8sTaskMainParameters);
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


    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        super.handle(taskCallBack);
        // put response in output
        Property outputProperty = new Property();
        outputProperty.setProp(PRE_NODE_OUTPUT);
        outputProperty.setDirect(Direct.OUT);
        outputProperty.setType(DataType.VARCHAR);
        outputProperty.setValue(String.valueOf(taskExecutionContext.getTaskInstanceId()));
        log.info("data set task call back:{},{}", outputProperty.getProp(), outputProperty.getValue());
        k8sTaskParameters.addPropertyToValPool(outputProperty);
    }

}
