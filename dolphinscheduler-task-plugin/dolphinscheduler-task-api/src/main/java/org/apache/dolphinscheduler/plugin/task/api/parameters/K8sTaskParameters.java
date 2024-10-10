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

    private String image;
    private String namespace;
    private String command;
    private String args;
    private String imagePullPolicy;
    private double minCpuCores;
    private double minMemorySpace;

    /**
     * 任务队列
     */
    private String queue;

    /**
     * GUP型号名称 默认:nvidia.com/gpu: 1
     * nvidia.com/r3090
     */
    private String gpuType;

    /**
     * gpu 资源配置
     */
    private Double gpuLimits;

    private List<Label> customizedLabels;
    private List<NodeSelectorExpression> nodeSelectors;

    /**
     * 对应页面上面的数据存储下拉，用于回显展示
     * */
    private String dataSave;

    /**
     * 类型是local/minio/...
     */
    private String fetchType;

    /**
     * 节点的输入输出挂载
     */
    private String fetchDataVolume;

    /**
     * 拉取数据的参数
     */
    private String fetchDataVolumeArgs;


    /**
     * 节点的输入输出挂载 包含宿主机 和 容器内部地址
     */
    private String podInputDataVolume;

    private String inputDataVolume;

    private String podOutputDataVolume;

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
