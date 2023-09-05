package org.apache.dolphinscheduler.plugin.task.api.k8s.impl;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_SUCCESS;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.UNIQUE_LABEL_NAME;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.utils.Serialization;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sFlinkOperatorTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.FlinkDeployment;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.utils.JobStatusEnums;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;

/**
 * @author lizu
 * @since 2022/7/30
 */
public class FlinkK8sOperatorTaskExecutor extends AbstractK8sTaskExecutor {

    private FlinkDeployment flinkDeployment;

    private K8sTaskExecutionContext k8sTaskExecutionContext;

    public FlinkK8sOperatorTaskExecutor(Logger logger, TaskExecutionContext taskRequest) {
        super(logger, taskRequest);
        this.k8sTaskExecutionContext = taskRequest.getK8sTaskExecutionContext();
    }

    @Override
    public TaskResponse run(String k8sParameterStr) throws Exception {
        TaskResponse result = new TaskResponse();
        int taskInstanceId = taskRequest.getTaskInstanceId();
        try {
            if (null == TaskExecutionContextCacheManager.getByTaskInstanceId(taskInstanceId)) {
                result.setExitStatusCode(EXIT_CODE_KILL);
                return result;
            }
            if (StringUtils.isEmpty(k8sParameterStr)) {
                TaskExecutionContextCacheManager.removeByTaskInstanceId(taskInstanceId);
                return result;
            }
            K8sTaskExecutionContext k8sTaskExecutionContext = taskRequest
                .getK8sTaskExecutionContext();
            String configYaml = k8sTaskExecutionContext.getConfigYaml();
            k8sUtils.buildClient(configYaml);
            submitJob2k8s(k8sParameterStr);
            //日志
            K8sFlinkOperatorTaskMainParameters k8STaskMainParameters = JSONUtils
                .parseObject(k8sParameterStr, K8sFlinkOperatorTaskMainParameters.class);
            registerBatchJobWatcher(Integer.toString(taskInstanceId), result,
                k8STaskMainParameters);
        } catch (Exception e) {
            cancelApplication(k8sParameterStr);
            Thread.currentThread().interrupt();
            result.setExitStatusCode(EXIT_CODE_FAILURE);
            throw e;
        }
        return result;
    }

    @Override
    public void cancelApplication(String k8sParameterStr) {
        log.info("cancel application operator job");
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
            log.info("[K8sOperatorJobExecutor-{}-{}] start to submit flink job", taskName,
                taskInstanceId);
            String asDeploymentYaml = asDeploymentYaml(flinkOperatorTaskMainParameters);
            stopJobOnK8s(k8sParameterStr);
            log.info("deploy yaml:{}", asDeploymentYaml);
            k8sUtils.loadFlinkOperatorJob(asDeploymentYaml);
            log.info("[K8sOperatorJobExecutor-{}-{}]  submitted flink job successfully", taskName,
                taskInstanceId);
        } catch (Exception e) {
            log.error("[K8sOperatorJobExecutor-{}-{}]  fail to submit flink job", taskName,
                taskInstanceId);
            throw new TaskException("K8sOperatorJobExecutor fail to submit flink job", e);
        }
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        String namespaceName = flinkDeployment.getMetadata().getNamespace();
        String jobName = flinkDeployment.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.flinkOperatorJobExist(jobName, namespaceName))) {
                K8sFlinkOperatorTaskMainParameters parameters = JSONUtils
                    .parseObject(k8sParameterStr, K8sFlinkOperatorTaskMainParameters.class);
                k8sUtils.deleteFlinkOperatorJob(asDeploymentYaml(parameters));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("[K8sJobExecutor-{}]  fail to stop flink job", jobName);
            throw new TaskException("K8sJobExecutor fail to stop flink job", e);
        }
    }


    private void registerBatchJobWatcher(String taskInstanceId, TaskResponse taskResponse,
        K8sFlinkOperatorTaskMainParameters k8STaskMainParameters) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Watcher<GenericKubernetesResource> watcher = new Watcher<GenericKubernetesResource>() {
            @Override
            public void eventReceived(Action action, GenericKubernetesResource resource) {
                try {
                    LogUtils.setTaskInstanceLogFullPathMDC(taskRequest.getLogPath());
                    log.info("event received : job:{} action:{}", resource.getMetadata().getName(),
                        action);
                    if (action != Action.ADDED) {
                        int jobStatus = getK8sJobStatus(resource);
                        log.info("watch flink operator :{}", jobStatus);
                        setTaskStatus(jobStatus, taskInstanceId, taskResponse,
                            k8STaskMainParameters);
                        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
                            countDownLatch.countDown();
                        }
                    } else if (action == Action.DELETED) {
                        log.error("[K8sJobExecutor-{}] fail in flink operator k8s",
                            resource.getMetadata().getName());
                        taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
                        countDownLatch.countDown();
                    }
                } finally {
                    LogUtils.removeTaskInstanceLogFullPathMDC();
                }
            }

            @Override
            public void onClose(WatcherException e) {
                log.info(String.format("[K8sJobExecutor-%s] fail in k8s: %s",
                    flinkDeployment.getMetadata().getName(), e.getMessage()));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
                countDownLatch.countDown();
            }

            @Override
            public void onClose() {
                log.warn("Watch gracefully closed");
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
        } catch (InterruptedException e) {
            log.error("operator flink job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } catch (Exception e) {
            log.error("operator flink job failed in k8s: {}", e.getMessage(), e);
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } finally {
            if (watch != null) {
                watch.close();
            }
        }
    }

    /**
     * Kind类型FlinkDeployment
     */
    private int getK8sJobStatus(GenericKubernetesResource resource) {
        Map<String, Object> additionalProperties = resource
            .getAdditionalProperties();
        if (additionalProperties != null) {
            Map<String, Object> status = (Map<String, Object>) additionalProperties
                .get("status");
            if (status != null) {
                Map<String, Object> applicationState = (Map<String, Object>) status
                    .get("jobStatus");
                if (applicationState != null) {
                    if (applicationState.get("state") != null) {
                        String state = applicationState.get("state").toString();
                        if (state.equals(JobStatusEnums.FINISHED.name())) {
                            return EXIT_CODE_SUCCESS;
                        } else if (state.equals(JobStatusEnums.FAILED.name())) {
                            return EXIT_CODE_FAILURE;
                        } else {
                            return TaskConstants.RUNNING_CODE;
                        }
                    }
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
                log.info(String.format("[K8sFlinkOperatorJobExecutor-%s] killed",
                    flinkDeployment.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                log.info(String.format("[K8sFlinkOperatorJobExecutor-%s] succeed in k8s",
                    flinkDeployment.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                    .getPodLog(flinkDeployment.getMetadata().getName(),
                        k8STaskMainParameters.getNamespaceName());
                log.info(String.format("[K8sFlinkOperatorJobExecutor-%s] fail in k8s: %s",
                    flinkDeployment.getMetadata().getName(), errorMessage));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
            }
        }
    }

    private String asDeploymentYaml(K8sFlinkOperatorTaskMainParameters parameters) {
        String flinkJobType = parameters.getFlinkJobType();
        switch (flinkJobType) {
            case TaskConstants.FLINK_SEATUNNEL:
                FlinkDeployment flinkDeployment = buildFlinkDeployment(parameters);
                ConfigMap configMap = buildSeaTunnelConf(parameters);
                return Serialization.asYaml(flinkDeployment) + Serialization.asYaml(configMap);
            case TaskConstants.FLINK_SQL:
                return null;
            default:
                throw new IllegalStateException("unknown flink job type for " + flinkJobType);
        }
    }

    private FlinkDeployment buildFlinkDeployment(K8sFlinkOperatorTaskMainParameters parameters) {
        this.flinkDeployment = getFlinkDeployment(parameters.getFlinkJobType());

        //设置镜像信息
        if(!StringUtils.isEmpty(parameters.getImage())){
            this.flinkDeployment.getSpec().setImage(parameters.getImage());
        }
        if(!StringUtils.isEmpty(parameters.getImagePullPolicy())){
            this.flinkDeployment.getSpec().setImagePullPolicy(parameters.getImagePullPolicy());
        }

        //设置job名称
        String k8sJobName = getK8sJobName(parameters);
        this.flinkDeployment.getMetadata().setName(k8sJobName);
        this.flinkDeployment.getMetadata()
            .setNamespace(k8sTaskExecutionContext.getNamespace().replace("_", ""));

        //设置job信息
        if (parameters.getParallelism() != null) {
            this.flinkDeployment.getSpec().getJob()
                .setParallelism(parameters.getParallelism());
        }
        //设置Flink 基本 参数
        if (parameters.getSlot() != null) {
            this.flinkDeployment.getSpec().getFlinkConfiguration()
                .put("taskmanager.numberOfTaskSlots", String.valueOf(parameters.getSlot()));
        }

        this.flinkDeployment.getSpec().getJobManager().getResource()
            .setMemory(parameters.getJobManagerMemory() == null ? "1024m"
                : parameters.getJobManagerMemory());
        this.flinkDeployment.getSpec().getTaskManager().getResource()
            .setMemory(parameters.getTaskManagerMemory() == null ? "1024m"
                : parameters.getTaskManagerMemory());
        this.flinkDeployment.getSpec().getJobManager().getResource()
            .setCpu(parameters.getJobManagerCpu() == null ? 0.5 : parameters.getJobManagerCpu());
        this.flinkDeployment.getSpec().getTaskManager().getResource()
            .setCpu(parameters.getTaskManagerCpu() == null ? 0.5 : parameters.getTaskManagerCpu());

        this.flinkDeployment.getSpec().getTaskManager()
            .setReplicas(parameters.getTaskManager() == null ? 1 : parameters.getTaskManager());

        //设置label
        Map<String, String> podLabelMap = new HashMap<>();
        podLabelMap.put(UNIQUE_LABEL_NAME, taskRequest.getTaskAppId());
        this.flinkDeployment.getSpec().getPodTemplate().getMetadata().setLabels(podLabelMap);

        //设置挂载参数
        this.flinkDeployment.getSpec().getPodTemplate().getSpec()
            .setVolumes(buildVolume(parameters.getFlinkJobType()));
        Container container = new Container();
        container.setName("flink-main-container");
        container.setVolumeMounts(buildVolumeMount(parameters.getFlinkJobType()));
        this.flinkDeployment.getSpec().getPodTemplate().getSpec()
            .setContainers(Arrays.asList(container));

        //设置环境变量
        if (!CollectionUtils.isEmpty(parameters.getParamsMap())) {
            Container containerEnv = this.flinkDeployment.getSpec().getPodTemplate().getSpec()
                .getContainers().get(0);
            containerEnv.setEnv(getEnv(parameters.getParamsMap()));
            this.flinkDeployment.getSpec().getPodTemplate().getSpec()
                .setContainers(Arrays.asList(containerEnv));
        }

        return flinkDeployment;
    }

    private List<Volume> buildVolume(String flinkJobType) {
        switch (flinkJobType) {
            case TaskConstants.FLINK_SEATUNNEL:
                VolumeBuilder volumeBuilder = new VolumeBuilder();
                volumeBuilder.withName(TaskConstants.SEATUNNEL_CONF_VOLUME_NAME);
                ConfigMapVolumeSourceBuilder configMap = new ConfigMapVolumeSourceBuilder();
                KeyToPath keyToPath = new KeyToPathBuilder()
                    .withKey(TaskConstants.SEATUNNEL_CONF_FILE)
                    .withPath(TaskConstants.SEATUNNEL_CONF_FILE)
                    .build();
                configMap.withName(geSeaTunnelConfigMapName())
                    .withItems(keyToPath);
                volumeBuilder.withConfigMap(configMap.build());
                return Arrays.asList(volumeBuilder.build());
            case TaskConstants.FLINK_SQL:
                return null;
            default:
                throw new IllegalStateException("unknown flink job type for " + flinkJobType);
        }
    }

    private List<VolumeMount> buildVolumeMount(String flinkJobType) {
        switch (flinkJobType) {
            case TaskConstants.FLINK_SEATUNNEL:
                VolumeMountBuilder seatunnelConf = new VolumeMountBuilder();
                seatunnelConf.withName(TaskConstants.SEATUNNEL_CONF_VOLUME_NAME);
                seatunnelConf.withMountPath(TaskConstants.SEATUNNEL_CONF_FILE_PATH);
                seatunnelConf.withSubPath(TaskConstants.SEATUNNEL_CONF_FILE);
                return Arrays.asList(seatunnelConf.build());
            case TaskConstants.FLINK_SQL:
                return null;
            default:
                throw new IllegalStateException("unknown flink job type for " + flinkJobType);
        }
    }

    /**
     * 根据模板文件获取FlinkDeployment
     */
    private static FlinkDeployment getFlinkDeployment(String flinkJobType) {
        try {
            InputStream resourceAsStream = null;
            switch (flinkJobType) {
                case TaskConstants.FLINK_SEATUNNEL:
                    resourceAsStream = FlinkK8sOperatorTaskExecutor.class
                        .getResourceAsStream("/seatunnle-on-k8s-operator.yaml");
                    return Serialization.yamlMapper()
                        .readValue(resourceAsStream, FlinkDeployment.class);
                case TaskConstants.FLINK_SQL:
                    resourceAsStream = FlinkK8sOperatorTaskExecutor.class
                        .getResourceAsStream("/flink-sql-on-k8s-operator.yaml");
                    return Serialization.yamlMapper()
                        .readValue(resourceAsStream, FlinkDeployment.class);
                default:
                    throw new IllegalStateException("unknown flink job type for " + flinkJobType);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String getK8sJobName(K8sFlinkOperatorTaskMainParameters parameters) {
        //设置job名称
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        return k8sJobName;
    }

    private ConfigMap buildSeaTunnelConf(K8sFlinkOperatorTaskMainParameters parameters) {
        //设置job名称
        String k8sJobName = getK8sJobName(parameters);
        ConfigMapBuilder builder = new ConfigMapBuilder();
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setName(k8sJobName);
        objectMeta.setNamespace(k8sTaskExecutionContext.getNamespace().replace("_", ""));
        Map<String, String> dataMap = new HashMap<>();
        dataMap.put(TaskConstants.SEATUNNEL_CONF_FILE, parameters.getRawScript());
        builder.withNewMetadataLike(objectMeta)
            .withName(geSeaTunnelConfigMapName())
            .endMetadata()
            .withData(dataMap);
        return builder.build();
    }

    private String geSeaTunnelConfigMapName() {
        //设置job名称
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String confName = String.format("%s-%s-seatunnel-configmap", taskName, taskInstanceId);
        return confName;
    }

    private List<EnvVar> getEnv(Map<String, String> paramsMap) {
        List<EnvVar> env = new ArrayList<>();
        for (Entry<String, String> entry : paramsMap.entrySet()) {
            EnvVar envVar = new EnvVar();
            envVar.setName(entry.getKey());
            envVar.setValue(entry.getValue());
            env.add(envVar);
        }
        return env;
    }
}
