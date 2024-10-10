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

package org.apache.dolphinscheduler.plugin.task.api.k8s;

import java.util.List;
import java.util.Map;

import lombok.Data;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;

/**
 * k8s task parameters
 */
@Data
public class K8sTaskMainParameters {

    private String image;
    private String command;
    private String args;
    private String namespaceName;
    private String clusterName;
    private String imagePullPolicy;
    private double minCpuCores;
    private double minMemorySpace;
    private Map<String, String> paramsMap;
    private Map<String, String> labelMap;
    private List<NodeSelectorRequirement> nodeSelectorRequirements;

    /**
     * 所选队列
     * */
    private String queue;

    /**
     * gpu 资源配置
     */
    private Double gpuLimits;

    /**
     * GUP型号名称 默认:nvidia.com/gpu: 1
     */
    private String gpuType;

    /**
     * 类型是local/minio/...
     * */
    private String fetchType;

    /**
     * 节点的输入输出挂载
     */
    private String fetchDataVolume;

    /**
     * 拉取数据的参数
     * */
    private String fetchDataVolumeArgs;

    /**
     * 对应页面上面的数据存储下拉，用于回显展示
     * */
    private String dataSave;

    /**
     * 节点的输入输出挂载 包含宿主机 和 容器内部地址
     */
    private String podInputDataVolume;

    private String inputDataVolume;

    private String podOutputDataVolume;

    private String outputDataVolume;

}
