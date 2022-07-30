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

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_ON_K8S_OPERATOR;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.FlinkK8sOperatorTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.K8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.SparkK8sOperatorTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;

public abstract class AbstractK8sTask extends AbstractTaskExecutor {

    /**
     * process task
     */
    private AbstractK8sTaskExecutor abstractK8sTaskExecutor;

    /**
     * Abstract k8s Task
     *
     * @param taskRequest taskRequest
     */
    protected AbstractK8sTask(TaskExecutionContext taskRequest) {
        super(taskRequest);
        this.abstractK8sTaskExecutor = new K8sTaskExecutor(logger, taskRequest);
    }


    protected AbstractK8sTask(TaskExecutionContext taskRequest, String type) {
        super(taskRequest);
        switch (type) {
            case FLINK_K8S_OPERATOR:
                this.abstractK8sTaskExecutor=new FlinkK8sOperatorTaskExecutor(logger,taskRequest);
                break;
            case SPARK_ON_K8S_OPERATOR:
                this.abstractK8sTaskExecutor = new SparkK8sOperatorTaskExecutor(logger,
                    taskRequest);
                break;
            default:
                this.abstractK8sTaskExecutor = new K8sTaskExecutor(logger, taskRequest);
        }
    }

    @Override
    public void handle() throws Exception {
        try {
            TaskResponse response = abstractK8sTaskExecutor.run(buildCommand());
            setExitStatusCode(response.getExitStatusCode());
            setAppIds(response.getAppIds());
        } catch (Exception e) {
            exitStatusCode = -1;
            throw new TaskException("k8s process failure", e);
        }
    }

    /**
     * cancel application
     *
     * @param status status
     * @throws Exception exception
     */
    @Override
    public void cancelApplication(boolean status) throws Exception {
        cancel = true;
        // cancel process
        abstractK8sTaskExecutor.cancelApplication(buildCommand());
    }

    /**
     * create command
     *
     * @return String
     * @throws Exception exception
     */
    protected abstract String buildCommand();

}
