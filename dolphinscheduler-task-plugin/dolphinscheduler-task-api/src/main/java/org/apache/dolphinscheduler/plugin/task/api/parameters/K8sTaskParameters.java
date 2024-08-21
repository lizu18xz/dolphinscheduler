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

package org.apache.dolphinscheduler.plugin.task.api.parameters;

import org.apache.dolphinscheduler.plugin.task.api.model.Label;
import org.apache.dolphinscheduler.plugin.task.api.model.NodeSelectorExpression;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

/**
 * k8s task parameters
 */
@Data
public class K8sTaskParameters extends AbstractParameters {

    /**
     * 任务类型 base/pytorch
     */
    private String k8sJobType;

    private String image;
    private String namespace;
    private String command;
    private String args;
    private String imagePullPolicy;
    private double minCpuCores;
    private double minMemorySpace;
    private List<Label> customizedLabels;
    private List<NodeSelectorExpression> nodeSelectors;

    /**
     * 节点的输入输出挂载
     */
    private String inputDataVolume;

    private String outputDataVolume;

    /**
     * 资源类型
     */
    private Boolean enableGpu;

    /**
     * 副本数量
     */
    private Integer masterReplicas;
    private Integer workerReplicas;

    /**
     * 资源配置
     */
    private String masterRequestsMemory;
    private String masterRequestsCpu;
    private String masterLimitsMemory;
    private String masterLimitsCpu;

    private String workerRequestsMemory;
    private String workerRequestsCpu;
    private String workerLimitsMemory;
    private String workerLimitsCpu;


    /**
     * resource list  所选的资源文件
     */
    private List<ResourceInfo> resourceList = new ArrayList<>();

    @Override
    public boolean checkParameters() {
        return StringUtils.isNotEmpty(image) && StringUtils.isNotEmpty(namespace);
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return new ArrayList<>();
    }
}
