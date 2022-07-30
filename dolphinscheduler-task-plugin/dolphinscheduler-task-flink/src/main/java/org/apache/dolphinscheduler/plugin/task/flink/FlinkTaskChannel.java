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

package org.apache.dolphinscheduler.plugin.task.flink;


import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ParametersNode;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

public class FlinkTaskChannel implements TaskChannel {
    @Override
    public void cancelApplication(boolean status) {

    }

    @Override
    public AbstractTask createTask(TaskExecutionContext taskRequest) {
        FlinkParameters flinkParameters = JSONUtils.parseObject(taskRequest.getTaskParams(), FlinkParameters.class);
        if(FLINK_K8S_OPERATOR.equals(flinkParameters.getDeployMode())){
            return new FlinkK8sOperatorTask(taskRequest);
        }
        return new FlinkTask(taskRequest);
    }

    @Override
    public AbstractParameters parseParameters(ParametersNode parametersNode) {
        return JSONUtils.parseObject(parametersNode.getTaskParams(), FlinkParameters.class);
    }

    @Override
    public ResourceParametersHelper getResources(String parameters) {
        return null;
    }
}
