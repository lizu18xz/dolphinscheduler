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

package org.apache.dolphinscheduler.plugin.task.k8s.dataset;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.DataSetK8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.FetchInfo;
import org.apache.dolphinscheduler.plugin.task.api.model.Label;
import org.apache.dolphinscheduler.plugin.task.api.model.NodeSelectorExpression;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.DataSetK8sTaskParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.common.constants.Constants.K8S_VOLUME;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.CLUSTER;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.NAMESPACE_NAME;

public class DataSetK8sTask extends AbstractK8sTask {

    /**
     * taskExecutionContext
     */
    private final TaskExecutionContext taskExecutionContext;

    /**
     * task parameters
     */
    private final DataSetK8sTaskParameters k8sTaskParameters;

    /**
     * @param taskRequest taskRequest
     */
    public DataSetK8sTask(TaskExecutionContext taskRequest) {
        super(taskRequest);
        this.taskExecutionContext = taskRequest;
        this.k8sTaskParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), DataSetK8sTaskParameters.class);
        log.info("Initialize data set k8s task parameters {}", JSONUtils.toPrettyJsonString(k8sTaskParameters));
        if (k8sTaskParameters == null || !k8sTaskParameters.checkParameters()) {
            throw new TaskException("data set K8S task params is not valid");
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
        DataSetK8sTaskMainParameters k8sTaskMainParameters = new DataSetK8sTaskMainParameters();
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

        //直接约定死
        String volumePrefix = PropertyUtils.getString(K8S_VOLUME) + "/" + taskExecutionContext.getProjectCode();
        //替换数据集参数
        String k8sFetchArgs = paramMap.get("k8s_fetch_args");
        if (!StringUtils.isEmpty(k8sFetchArgs)) {
            log.info("k8s_fetch_args string :{}",k8sFetchArgs);
            List<FetchInfo> fetchInfos = JSONUtils.parseObject(k8sFetchArgs, new TypeReference<List<FetchInfo>>() {
            });
            log.info("k8s_fetch_args fetchInfos:{},{}", fetchInfos.size(),JSONUtils.toJsonString(fetchInfos));
            fetchInfos = fetchInfos.stream().map(x -> {
                x.setFetchDataVolume(volumePrefix + "/fetch/");
                return x;
            }).collect(Collectors.toList());
            //替换模版中的参数
            k8sTaskParameters.setFetchInfos(fetchInfos);
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

        if (!CollectionUtils.isEmpty(k8sTaskParameters.getFetchInfos())) {
            List<FetchInfo> fetchInfos = k8sTaskParameters.getFetchInfos();
            fetchInfos = fetchInfos.stream().map(x -> {
                x.setFetchDataVolume(volumePrefix + "/fetch/");
                return x;
            }).collect(Collectors.toList());
            k8sTaskMainParameters.setFetchInfos(fetchInfos);
        }
        //设置约定的挂载信息
        k8sTaskMainParameters.setOutputDataVolume(volumePrefix + "/output/");
        k8sTaskMainParameters.setInputDataVolume(volumePrefix + "/fetch/");
        k8sTaskMainParameters.setPodInputDataVolume("/data/input");
        k8sTaskMainParameters.setPodOutputDataVolume("/data/output");

        k8sTaskMainParameters.setGpuType(k8sTaskParameters.getGpuType());
        k8sTaskMainParameters.setGpuLimits(k8sTaskParameters.getGpuLimits());
        k8sTaskMainParameters.setQueue(k8sTaskParameters.getQueue());
        k8sTaskMainParameters.setMultiple(k8sTaskParameters.getMultiple());

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

}
