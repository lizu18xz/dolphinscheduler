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

import static org.apache.dolphinscheduler.common.constants.Constants.ENABLE_K8S_QUEUE;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.PYTORCH_K8S_OPERATOR;

import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractRemoteTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.*;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;

public abstract class AbstractK8sTask extends AbstractRemoteTask {

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
        Boolean enable = PropertyUtils.getBoolean(ENABLE_K8S_QUEUE, false);
        if (enable) {
            String taskType = taskRequest.getTaskType();
            if (taskType.equalsIgnoreCase("DATA_SET_K8S")) {
                this.abstractK8sTaskExecutor = new DataSetK8sQueueTaskExecutor(log, taskRequest);
            } else {
                log.info("k8s queue enable");
                this.abstractK8sTaskExecutor = new K8sQueueTaskExecutor(log, taskRequest);
            }
        } else {
            this.abstractK8sTaskExecutor = new K8sTaskExecutor(log, taskRequest);
        }
    }

    protected AbstractK8sTask(TaskExecutionContext taskRequest, String type) {
        super(taskRequest);
        switch (type) {
            case FLINK_K8S_OPERATOR:
                this.abstractK8sTaskExecutor = new FlinkK8sOperatorTaskExecutor(log, taskRequest);
                break;
            case PYTORCH_K8S_OPERATOR:
                Boolean enable = PropertyUtils.getBoolean(ENABLE_K8S_QUEUE, false);
                if (enable) {
                    this.abstractK8sTaskExecutor = new PytorchK8sQueueTaskExecutor(log, taskRequest);
                } else {
                    this.abstractK8sTaskExecutor = new PytorchK8sOperatorTaskExecutor(log, taskRequest);
                }
                break;
            default:
                this.abstractK8sTaskExecutor = new K8sTaskExecutor(log, taskRequest);
        }
    }

    // todo split handle to submit and track
    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        try {
            TaskResponse response = abstractK8sTaskExecutor.run(buildCommand());
            setExitStatusCode(response.getExitStatusCode());
            setAppIds(response.getAppIds());
        } catch (Exception e) {
            log.error("k8s task submit failed with error");
            exitStatusCode = -1;
            throw new TaskException("Execute k8s task error", e);
        }
    }

    // todo
    @Override
    public void submitApplication() throws TaskException {

    }

    // todo
    @Override
    public void trackApplicationStatus() throws TaskException {

    }

    /**
     * cancel application
     *
     * @throws Exception exception
     */
    @Override
    public void cancelApplication() throws TaskException {
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
