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
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.DataType;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.DataSetK8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.*;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.DataSetK8sTaskParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.common.constants.Constants.K8S_VOLUME;
import static org.apache.dolphinscheduler.common.constants.Constants.PRE_NODE_OUTPUT;
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
        log.info("dataset command paramsMap:{}", JSONUtils.toJsonString(paramsMap));
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
            log.info("k8s_fetch_args string :{}", k8sFetchArgs);
            List<FetchInfo> fetchInfos = JSONUtils.parseObject(k8sFetchArgs, new TypeReference<List<FetchInfo>>() {
            });
            log.info("k8s_fetch_args fetchInfos:{},{}", fetchInfos.size(), JSONUtils.toJsonString(fetchInfos));
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

        //获取s3参数，转换为fetchInfos
        List<S3FetchInfo> s3FetchInfos = k8sTaskParameters.getS3FetchInfos();
        if (!CollectionUtils.isEmpty(s3FetchInfos)) {
            log.info("s3 fetch:{}", s3FetchInfos.size());
            List<FetchInfo> fetchInfos = new ArrayList<>();
            for (S3FetchInfo s3FetchInfo : s3FetchInfos) {
                FetchInfo fetchInfo = new FetchInfo();
                StringBuilder args = new StringBuilder();
                args.append("[").append("\"").append("obs").append("\"").append(",").append("\"")
                        .append(s3FetchInfo.getHost()).append("\"").append(",").append("\"")
                        .append(s3FetchInfo.getAppKey()).append("\"").append(",").append("\"")
                        .append(s3FetchInfo.getAppSecret()).append("\"").append(",").append("\"")
                        .append(s3FetchInfo.getBucketName()).append("\"").append(",").append("\"")
                        .append(s3FetchInfo.getPath()).append("\"").append(",")
                        //容器内部地址写死
                        .append("\"").append("/app/downloads").append("\"").append(",").append("]");
                fetchInfo.setFetchDataVolumeArgs(args.toString());
                fetchInfos.add(fetchInfo);
            }
            k8sTaskParameters.setFetchInfos(fetchInfos);
        }

        String inputDataVolume = volumePrefix + "/fetch/";
        if (!CollectionUtils.isEmpty(k8sTaskParameters.getFetchInfos())) {
            List<FetchInfo> fetchInfos = k8sTaskParameters.getFetchInfos();
            fetchInfos = fetchInfos.stream().map(x -> {
                x.setFetchDataVolume(volumePrefix + "/fetch/");
                return x;
            }).collect(Collectors.toList());
            k8sTaskMainParameters.setFetchInfos(fetchInfos);
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

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        super.handle(taskCallBack);
        // put response in output
        Property outputProperty = new Property();
        //outputProperty.setProp(String.format("%s_%s", taskExecutionContext.getTaskName(), "output"));
        outputProperty.setProp(PRE_NODE_OUTPUT);
        outputProperty.setDirect(Direct.OUT);
        outputProperty.setType(DataType.VARCHAR);
        outputProperty.setValue(String.valueOf(taskExecutionContext.getTaskInstanceId()));
        log.info("data set task call back:{},{}", outputProperty.getProp(), outputProperty.getValue());
        k8sTaskParameters.addPropertyToValPool(outputProperty);
    }
}
