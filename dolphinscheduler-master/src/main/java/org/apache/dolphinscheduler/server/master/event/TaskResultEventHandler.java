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

package org.apache.dolphinscheduler.server.master.event;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.enums.StateEventType;
import org.apache.dolphinscheduler.common.enums.TaskEventType;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.K8sQueueTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.apache.dolphinscheduler.dao.utils.TaskInstanceUtils;
import org.apache.dolphinscheduler.extract.base.client.SingletonJdkDynamicRpcClientProxyFactory;
import org.apache.dolphinscheduler.extract.worker.ITaskInstanceExecutionEventAckListener;
import org.apache.dolphinscheduler.extract.worker.transportor.TaskInstanceExecutionFinishEventAck;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sTaskParameters;
import org.apache.dolphinscheduler.server.master.cache.ProcessInstanceExecCacheManager;
import org.apache.dolphinscheduler.server.master.config.MasterConfig;
import org.apache.dolphinscheduler.server.master.processor.queue.TaskEvent;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteRunnable;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteThreadPool;
import org.apache.dolphinscheduler.server.master.utils.DataQualityResultOperator;
import org.apache.dolphinscheduler.server.master.utils.HttpRequestUtil;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.dolphinscheduler.common.constants.Constants.*;

@Slf4j
@Component
public class TaskResultEventHandler implements TaskEventHandler {

    @Autowired
    private ProcessInstanceExecCacheManager processInstanceExecCacheManager;

    @Autowired
    private WorkflowExecuteThreadPool workflowExecuteThreadPool;

    @Autowired
    private DataQualityResultOperator dataQualityResultOperator;

    @Autowired
    private ProcessService processService;

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private MasterConfig masterConfig;

    @Autowired
    private K8sQueueTaskDao k8sQueueTaskDao;

    @Autowired
    private ProjectDao projectDao;

    @Override
    public void handleTaskEvent(TaskEvent taskEvent) throws TaskEventHandleError, TaskEventHandleException {
        int taskInstanceId = taskEvent.getTaskInstanceId();
        int processInstanceId = taskEvent.getProcessInstanceId();

        WorkflowExecuteRunnable workflowExecuteRunnable = this.processInstanceExecCacheManager.getByProcessInstanceId(
                processInstanceId);
        if (workflowExecuteRunnable == null) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task result event error, cannot find related workflow instance from cache, will discard this event");
        }
        Optional<TaskInstance> taskInstanceOptional = workflowExecuteRunnable.getTaskInstance(taskInstanceId);
        if (!taskInstanceOptional.isPresent()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task result event error, cannot find the taskInstance from cache, will discord this event");
        }
        TaskInstance taskInstance = taskInstanceOptional.get();
        if (taskInstance.getState().isFinished()) {
            sendAckToWorker(taskEvent);
            throw new TaskEventHandleError(
                    "Handle task result event error, the task instance is already finished, will discord this event");
        }
        dataQualityResultOperator.operateDqExecuteResult(taskEvent, taskInstance);

        TaskInstance oldTaskInstance = new TaskInstance();
        TaskInstanceUtils.copyTaskInstance(taskInstance, oldTaskInstance);
        try {
            taskInstance.setStartTime(taskEvent.getStartTime());
            taskInstance.setHost(taskEvent.getWorkerAddress());
            taskInstance.setLogPath(taskEvent.getLogPath());
            taskInstance.setExecutePath(taskEvent.getExecutePath());
            taskInstance.setPid(taskEvent.getProcessId());
            taskInstance.setAppLink(taskEvent.getAppIds());
            taskInstance.setState(taskEvent.getState());
            taskInstance.setEndTime(taskEvent.getEndTime());
            taskInstance.setVarPool(taskEvent.getVarPool());
            processService.changeOutParam(taskInstance);
            log.info("Handle task result event updateById:{}", JSONUtils.toJsonString(taskInstance));
            taskInstanceDao.updateById(taskInstance);
            updateK8sQueueAndDoHttp(taskInstance);
            sendAckToWorker(taskEvent);
        } catch (Exception ex) {
            TaskInstanceUtils.copyTaskInstance(oldTaskInstance, taskInstance);
            throw new TaskEventHandleError("Handle task result event error, save taskInstance to db error", ex);
        }
        TaskStateEvent stateEvent = TaskStateEvent.builder()
                .processInstanceId(taskEvent.getProcessInstanceId())
                .taskInstanceId(taskEvent.getTaskInstanceId())
                .status(taskEvent.getState())
                .type(StateEventType.TASK_STATE_CHANGE)
                .build();
        workflowExecuteThreadPool.submitStateEvent(stateEvent);

    }

    private void updateK8sQueueAndDoHttp(TaskInstance taskInstance) {
        try {
            Boolean enable = PropertyUtils.getBoolean(ENABLE_K8S_QUEUE, false);
            if (!enable) {
                return;
            }
            //判断是否是K8s任务
            String taskType = taskInstance.getTaskType();
            if (taskType.equals("K8S") || taskType.equals("PYTORCH_K8S") || taskType.equals("DATA_SET_K8S")) {
                long taskCode = taskInstance.getTaskCode();
                k8sQueueTaskDao.updateStatus(taskCode, "待下次运行");
            }
            //进行回调，如果存在输出，则通知其上传到对象存储中
            if (taskType.equals("K8S") || taskType.equals("DATA_SET_K8S")) {
                K8sTaskParameters k8sTaskParameters =
                        JSONUtils.parseObject(taskInstance.getTaskParams(), K8sTaskParameters.class);
                log.info("k8sTaskParameters:{}", JSONUtils.toJsonString(k8sTaskParameters));
                String outputVolumeNameInfo = k8sTaskParameters.getOutputVolumeNameInfo();
                String address =
                        PropertyUtils.getString(TASK_UPLOAD_ADDRESS);
                if (StringUtils.isEmpty(address)) {
                    throw new IllegalArgumentException("task.upload.address not found");
                }
                String projectEnName = "";
                List<Project> projects = projectDao.queryByCodes(Lists.newArrayList(taskInstance.getProjectCode()));
                if (!CollectionUtils.isEmpty(projects)) {
                    Project project = projects.get(0);
                    projectEnName = project.getProjectEnName();
                }
                Map<String, Object> outputInfoMap = new HashMap<>();
                if (!StringUtils.isEmpty(outputVolumeNameInfo)) {
                    outputInfoMap = JSONUtils.toMap(outputVolumeNameInfo, String.class, Object.class);
                    outputInfoMap.put("projectName", projectEnName);
                    outputInfoMap.put("type", 1);
                    outputInfoMap.put("dataId", outputInfoMap.get("tpDatasetId"));
                    //获取输出路径
                    String taskOutPutPath = PropertyUtils.getString(K8S_VOLUME) + "/" + taskInstance.getProjectCode()
                            + "/output/" + taskInstance.getId();
                    outputInfoMap.put("localFilePath", taskOutPutPath);
                    outputInfoMap.put("path", outputInfoMap.get("filePath"));
                    outputInfoMap.put("key", outputInfoMap.get("appKey"));
                    outputInfoMap.put("appSecret", outputInfoMap.get("appSecret"));

                    outputInfoMap.put("dataType", k8sTaskParameters.getDataType());

                    //源ID
                    outputInfoMap.put("sourceId", k8sTaskParameters.getFetchId());
                    outputInfoMap.put("workFlowId", taskInstance.getProcessDefine().getCode());

                } else {
                    String modelId = k8sTaskParameters.getModelId();
                    outputInfoMap = new HashMap<>();
                    outputInfoMap.put("projectName", projectEnName);
                    outputInfoMap.put("type", 0);
                    outputInfoMap.put("modelId", modelId);
                    String taskOutPutPath = PropertyUtils.getString(K8S_VOLUME) + "/" + taskInstance.getProjectCode()
                            + "/output/" + taskInstance.getId();
                    outputInfoMap.put("localFilePath", taskOutPutPath);
                    outputInfoMap.put("workFlowId", taskInstance.getProcessDefine().getCode());
                }
                log.info("request map:{}", JSONUtils.toJsonString(outputInfoMap));
                HttpPost httpPost = HttpRequestUtil.constructHttpPost(address, JSONUtils.toJsonString(outputInfoMap));
                CloseableHttpClient httpClient;
                httpClient = HttpRequestUtil.getHttpClient();
                CloseableHttpResponse response = null;
                try {
                    httpPost.setHeader("token", "cdd8c9bab1596b12dbe45ecb6979bc95");
                    response = httpClient.execute(httpPost);
                    int statusCode = response.getStatusLine().getStatusCode();
                    if (statusCode != HttpStatus.SC_OK) {
                        log.error("return http status code: {} ", statusCode);
                    }
                    String resp;
                    HttpEntity entity = response.getEntity();
                    resp = EntityUtils.toString(entity, "utf-8");
                    log.info("output resp :{}", resp.toString());
                } catch (Exception e) {
                    log.error("output error:{},e:{}", "", e);
                } finally {
                    try {
                        response.close();
                        httpClient.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("updateK8sQueueAndDoHttp error:{}", e);
        }
    }

    public void sendAckToWorker(TaskEvent taskEvent) {
        ITaskInstanceExecutionEventAckListener instanceExecutionEventAckListener =
                SingletonJdkDynamicRpcClientProxyFactory.getInstance()
                        .getProxyClient(taskEvent.getWorkerAddress(), ITaskInstanceExecutionEventAckListener.class);
        instanceExecutionEventAckListener.handleTaskInstanceExecutionFinishEventAck(
                TaskInstanceExecutionFinishEventAck.success(taskEvent.getTaskInstanceId()));
    }

    @Override
    public TaskEventType getHandleEventType() {
        return TaskEventType.RESULT;
    }
}
