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

import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.Data;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;

/**
 * k8s task parameters
 */
@Data
public class K8sPytorchTaskMainParameters {

    private String image;
    private String command;
    private String args;
    private String namespaceName;
    private String clusterName;
    private String imagePullPolicy;

    private Map<String, String> paramsMap;
    private Map<String, String> labelMap;
    private List<NodeSelectorRequirement> nodeSelectorRequirements;


    /**
     * 副本数量
     */
    private Integer masterReplicas = 1;
    private Integer workerReplicas;

    /**
     * gpu resources 设置
     */
    private Double masterGpuLimits;

    /**
     * gpu resources 设置
     */
    private Double workerGpuLimits;
    /**
     * 资源配置
     */
    private double masterMinCpuCores;
    private double masterMinMemorySpace;

    private double workerMinCpuCores;
    private double workerMinMemorySpace;

    /**
     * 节点的输入输出挂载
     */
    private String inputDataVolume;

    private String outputDataVolume;

    /**
     * resource list  所选的资源文件
     */
    private List<ResourceInfo> resourceList = new ArrayList<>();

}
