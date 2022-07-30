package org.apache.dolphinscheduler.plugin.task.api.k8s.impl;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_SUCCESS;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR_COMPLETED;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR_FAILED;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sFlinkOperatorTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flink.FlinkDeployment;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.slf4j.Logger;

/**
 * @author lizu
 * @since 2022/7/30
 */
public class FlinkK8sOperatorTaskExecutor extends AbstractK8sTaskExecutor {

    private FlinkDeployment flinkDeployment;

    public FlinkK8sOperatorTaskExecutor(Logger logger, TaskExecutionContext taskRequest) {
        super(logger, taskRequest);
    }

    @Override
    public TaskResponse run(String k8sParameterStr) throws Exception {
        TaskResponse result = new TaskResponse();
        int taskInstanceId = taskRequest.getTaskInstanceId();
        K8sFlinkOperatorTaskMainParameters k8STaskMainParameters = JSONUtils
            .parseObject(k8sParameterStr, K8sFlinkOperatorTaskMainParameters.class);
        try {
            if (null == TaskExecutionContextCacheManager.getByTaskInstanceId(taskInstanceId)) {
                result.setExitStatusCode(EXIT_CODE_KILL);
                return result;
            }
            if (StringUtils.isEmpty(k8sParameterStr)) {
                TaskExecutionContextCacheManager.removeByTaskInstanceId(taskInstanceId);
                return result;
            }
            //TODO 认证连接
            k8sUtils.buildNoAuthClient(k8STaskMainParameters.getMasterUrl());
            submitJob2k8s(k8sParameterStr);
            registerBatchJobWatcher(Integer.toString(taskInstanceId), result,
                k8STaskMainParameters);
        } catch (Exception e) {
            result.setExitStatusCode(EXIT_CODE_FAILURE);
            throw e;
        }
        return result;
    }

    private void registerBatchJobWatcher(String taskInstanceId, TaskResponse taskResponse,
        K8sFlinkOperatorTaskMainParameters k8STaskMainParameters) {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Watcher<GenericKubernetesResource> watcher = new Watcher<GenericKubernetesResource>() {
            @Override
            public void eventReceived(Action action, GenericKubernetesResource resource) {
                if (action != Action.ADDED) {
                    int jobStatus = getK8sJobStatus(resource);
                    logger.info("watch flink operator :{}", jobStatus);
                    setTaskStatus(jobStatus, taskInstanceId, taskResponse, k8STaskMainParameters);
                    if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
                        countDownLatch.countDown();
                    }
                }
            }

            @Override
            public void onClose(WatcherException e) {
                logStringBuffer.append(String.format("[K8sJobExecutor-%s] fail in k8s: %s",
                    flinkDeployment.getMetadata().getName(), e.getMessage()));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
                countDownLatch.countDown();
            }

            @Override
            public void onClose() {
                logger.warn("Watch gracefully closed");
            }
        };

        Watch watch = null;
        try {
            watch = k8sUtils.createBatchFlinkOperatorJobWatcher(
                flinkDeployment.getMetadata().getName(), watcher);
            boolean timeoutFlag = taskRequest.getTaskTimeoutStrategy() == TaskTimeoutStrategy.FAILED
                || taskRequest.getTaskTimeoutStrategy() == TaskTimeoutStrategy.WARNFAILED;
            if (timeoutFlag) {
                Boolean timeout = !(countDownLatch
                    .await(taskRequest.getTaskTimeout(), TimeUnit.SECONDS));
                waitTimeout(timeout);
            } else {
                countDownLatch.await();
            }
            flushLog(taskResponse);
        } catch (InterruptedException e) {
            logger.error("operator flink job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } catch (Exception e) {
            logger.error("operator flink job failed in k8s: {}", e.getMessage(), e);
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } finally {
            if (watch != null) {
                watch.close();
            }
        }

    }

    private int getK8sJobStatus(GenericKubernetesResource resource) {
        Map<String, Object> additionalProperties = resource
            .getAdditionalProperties();
        if (additionalProperties != null) {
            Map<String, Object> status = (Map<String, Object>) additionalProperties
                .get("status");
            Map<String, Object> applicationState = (Map<String, Object>) status
                .get("jobStatus");
            if (applicationState != null) {
                String state = applicationState.get("state").toString();
                if (state.equals(FLINK_K8S_OPERATOR_COMPLETED)) {
                    return EXIT_CODE_SUCCESS;
                } else if (state.equals(FLINK_K8S_OPERATOR_FAILED)) {
                    return EXIT_CODE_FAILURE;
                } else {
                    return TaskConstants.RUNNING_CODE;
                }
            }
        }
        return TaskConstants.RUNNING_CODE;
    }

    private void setTaskStatus(int jobStatus, String taskInstanceId, TaskResponse taskResponse,
        K8sTaskMainParameters k8STaskMainParameters) {
        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
            if (null == TaskExecutionContextCacheManager
                .getByTaskInstanceId(Integer.valueOf(taskInstanceId))) {
                logStringBuffer.append(String.format("[K8sFlinkOperatorJobExecutor-%s] killed",
                    flinkDeployment.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                logStringBuffer
                    .append(String.format("[K8sFlinkOperatorJobExecutor-%s] succeed in k8s",
                        flinkDeployment.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                    .getPodLog(flinkDeployment.getMetadata().getName(),
                        k8STaskMainParameters.getNamespaceName());
                logStringBuffer
                    .append(String.format("[K8sFlinkOperatorJobExecutor-%s] fail in k8s: %s",
                        flinkDeployment.getMetadata().getName(), errorMessage));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
            }
        }
    }

    private FlinkDeployment buildFlinkDeployment(K8sFlinkOperatorTaskMainParameters parameters) {
        FlinkDeployment flinkDeployment = getFlinkDeployment();
        //设置job名称
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        flinkDeployment.getMetadata().setName(k8sJobName);

        //设置环境变量
        Container container = new Container();
        container.setEnv(getEnv(parameters.getParamsMap()));
        this.flinkDeployment.getSpec().getPodTemplate().getSpec().getContainers().add(container);

        //设置函数信息
        this.flinkDeployment.getSpec().getJob()
            .setParallelism(parameters.getParallelism());
        this.flinkDeployment.getSpec().getJob().setEntryClass(parameters.getMainClass());

        //设置基本 参数
        this.flinkDeployment.getSpec().getJobManager().getResource()
            .setMemory(parameters.getTaskManagerMemory());
        this.flinkDeployment.getSpec().getTaskManager().getResource()
            .setMemory(parameters.getTaskManagerMemory());
        this.flinkDeployment.getSpec().getFlinkConfiguration()
            .put("taskmanager.numberOfTaskSlots", String.valueOf(parameters.getSlot()));
        return flinkDeployment;

    }

    private List<EnvVar> getEnv(Map<String, String> paramsMap) {
        List<EnvVar> env = new ArrayList<>();
        for (Entry<String, String> entry : paramsMap.entrySet()) {
            EnvVar envVar = new EnvVar();
            envVar.setName(entry.getKey());
            envVar.setName(entry.getValue());
            env.add(envVar);
        }
        return env;
    }

    public static FlinkDeployment getFlinkDeployment() {
        InputStream resourceAsStream = FlinkK8sOperatorTaskExecutor.class
            .getResourceAsStream("/flink-pod-template.yaml");
        try {
            FlinkDeployment flinkDeployment = SerializationUtils.getMapper()
                .readValue(resourceAsStream, FlinkDeployment.class);
            return flinkDeployment;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void cancelApplication(String k8sParameterStr) {
        if (flinkDeployment != null) {
            stopJobOnK8s(k8sParameterStr);
        }
    }

    @Override
    public void submitJob2k8s(String k8sParameterStr) {
        int taskInstanceId = taskRequest.getTaskInstanceId();
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        K8sFlinkOperatorTaskMainParameters flinkOperatorTaskMainParameters = JSONUtils
            .parseObject(k8sParameterStr, K8sFlinkOperatorTaskMainParameters.class);

        try {
            logger.info("[K8sOperatorJobExecutor-{}-{}] start to submit flink job", taskName,
                taskInstanceId);

            flinkDeployment = buildFlinkDeployment(flinkOperatorTaskMainParameters);

            stopJobOnK8s(k8sParameterStr);

            k8sUtils.createFlinkOperatorJob(flinkDeployment.getMetadata().getNamespace(),
                flinkDeployment);
            logger
                .info("[K8sOperatorJobExecutor-{}-{}]  submitted flink job successfully", taskName,
                    taskInstanceId);
        } catch (Exception e) {
            logger.error("[K8sOperatorJobExecutor-{}-{}]  fail to submit flink job", taskName,
                taskInstanceId);
            throw new TaskException("K8sOperatorJobExecutor fail to submit flink job", e);
        }

    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        K8sFlinkOperatorTaskMainParameters flinkOperatorTaskMainParameters = JSONUtils
            .parseObject(k8sParameterStr, K8sFlinkOperatorTaskMainParameters.class);
        String namespaceName = flinkOperatorTaskMainParameters.getNamespaceName();
        String jobName = flinkDeployment.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.sparkOperatorJobExist(jobName, namespaceName))) {
                k8sUtils.deleteFlinkOperatorJob(namespaceName, flinkDeployment);
            }
        } catch (Exception e) {
            logger.error("[K8sJobExecutor-{}]  fail to stop flink job", jobName);
            throw new TaskException("K8sJobExecutor fail to stop flink job", e);
        }
    }
}
