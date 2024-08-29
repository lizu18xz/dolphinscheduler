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

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.model.Label;
import org.apache.dolphinscheduler.plugin.task.api.model.NodeSelectorExpression;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * k8s task parameters
 */
@Data
public class K8sPytorchTaskParameters extends AbstractParameters {

    /**
     * 任务类型 base/pytorch
     */
    private String k8sJobType;

    private String image;
    private String namespace;
    private String command;
    private String args;
    private String imagePullPolicy;
    private List<Label> customizedLabels;
    private List<NodeSelectorExpression> nodeSelectors;

    private String volumeType;


    /**
     * 副本数量
     */
    private Integer masterReplicas = 1;
    private Integer workerReplicas;

    /**
     * gpu resources 设置
     */
    private String masterGpuLimits;

    /**
     * gpu resources 设置
     */
    private String workerGpuLimits;
    /**
     * 资源配置
     */
    private double masterMinCpuCores;
    private double masterMinMemorySpace;

    private String workerMinCpuCores;
    private String workerMinMemorySpace;

    /**
     * 节点的输入输出挂载
     */
    private String inputDataVolume;

    private String outputDataVolume;

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
