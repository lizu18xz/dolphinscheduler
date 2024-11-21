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
import org.apache.dolphinscheduler.plugin.task.api.model.*;

import java.util.ArrayList;
import java.util.List;

/**
 * k8s task parameters
 */
@Data
public class DataSetK8sTaskParameters extends AbstractParameters {

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
     * 前置拉取数据信息,数据来源
     */
    private List<FetchInfo> fetchInfos;

    private List<S3FetchInfo> s3FetchInfos;

    /**
     * 是否多pod运行
     */
    private Boolean multiple;

    /**
     * 数据存储相关信息,最终文件要上传的地方
     */
    private String outputVolumeId;

    private String outputVolumeName;

    private String outputVolumeNameInfo;

    /**
     * 模型ID
     */
    private String modelId;


    /**
     * 数据集相关参数
     */
    private String sourceId;

    private String dataType;


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
