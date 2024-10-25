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

package org.apache.dolphinscheduler.plugin.task.api.k8s.impl;

import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJob;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJobSpec;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.MapUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.ProcessUtils;
import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.dolphinscheduler.common.constants.Constants.*;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.*;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_SUCCESS;

/**
 * K8sTaskExecutor used to submit k8s task to K8S
 */
public class K8sQueueTaskExecutor extends AbstractK8sTaskExecutor {

    private QueueJob job;
    protected boolean podLogOutputIsFinished = false;
    protected Future<?> podLogOutputFuture;

    public K8sQueueTaskExecutor(Logger logger, TaskExecutionContext taskRequest) {
        super(logger, taskRequest);
    }

    /**
     * 构建有队列的任务
     */
    public QueueJob buildK8sQueueJob(K8sTaskMainParameters k8STaskMainParameters) {
        String taskType = taskRequest.getTaskType();
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String image = k8STaskMainParameters.getImage();
        String namespaceName = k8STaskMainParameters.getNamespaceName();
        String imagePullPolicy = k8STaskMainParameters.getImagePullPolicy();
        Map<String, String> otherParams = k8STaskMainParameters.getParamsMap();
        String queue = k8STaskMainParameters.getQueue() == null ? "default" : k8STaskMainParameters.getQueue();
        //设置资源
        Map<String, Quantity> limitRes = new HashMap<>();
        Map<String, Quantity> reqRes = new HashMap<>();
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        if (k8STaskMainParameters.getGpuLimits() == null || k8STaskMainParameters.getGpuLimits() <= 0) {
            Double podMem = k8STaskMainParameters.getMinMemorySpace();
            Double podCpu = k8STaskMainParameters.getMinCpuCores();
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            reqRes.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            reqRes.put(CPU, new Quantity(String.valueOf(podCpu)));
            limitRes.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            limitRes.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
        } else {
            //nvidia.com/gpu: 1
            String gpuType = k8STaskMainParameters.getGpuType();
            Double podGpu = k8STaskMainParameters.getGpuLimits();
            if (StringUtils.isEmpty(gpuType)) {
                gpuType = GPU;
            }
            limitRes.put(gpuType, new Quantity(String.valueOf(podGpu)));
        }

        Map<String, String> labelMap = k8STaskMainParameters.getLabelMap();
        labelMap.put(LAYER_LABEL, LAYER_LABEL_VALUE);
        labelMap.put(NAME_LABEL, k8sJobName);
        Map<String, String> podLabelMap = new HashMap<>();
        podLabelMap.put(UNIQUE_LABEL_NAME, taskRequest.getTaskAppId());

        EnvVar taskInstanceIdVar = new EnvVar(TASK_INSTANCE_ID, taskInstanceId, null);
        List<EnvVar> envVars = new ArrayList<>();
        envVars.add(taskInstanceIdVar);
        if (MapUtils.isNotEmpty(otherParams)) {
            for (Map.Entry<String, String> entry : otherParams.entrySet()) {
                String param = entry.getKey();
                String paramValue = entry.getValue();
                EnvVar envVar = new EnvVar(param, paramValue, null);
                envVars.add(envVar);
            }
        }

        String commandString = k8STaskMainParameters.getCommand();
        String argsString = k8STaskMainParameters.getArgs();
        List<String> commands = new ArrayList<>();
        List<String> args = new ArrayList<>();

        try {
            if (!StringUtils.isEmpty(commandString)) {
                commands = yaml.load(commandString.trim());
            }
            if (!StringUtils.isEmpty(argsString)) {
                args = yaml.load(argsString.trim());
            }
        } catch (Exception e) {
            throw new TaskException("Parse yaml-like commands and args failed", e);
        }

        NodeSelectorTerm nodeSelectorTerm = new NodeSelectorTerm();
        nodeSelectorTerm.setMatchExpressions(k8STaskMainParameters.getNodeSelectorRequirements());

        Affinity affinity = k8STaskMainParameters.getNodeSelectorRequirements().size() == 0 ? null
                : new AffinityBuilder()
                .withNewNodeAffinity()
                .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                .addNewNodeSelectorTermLike(nodeSelectorTerm)
                .endNodeSelectorTerm()
                .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity().build();

        //设置容器挂载
        List<VolumeMount> volumeMounts = new ArrayList<>();
        //设置宿主机挂载
        List<Volume> volumes = new ArrayList<>();
        //必须有数据来源，这里才会有前置挂载
        if (!StringUtils.isEmpty(k8STaskMainParameters.getFetchDataVolume())) {
            //容器
            VolumeMount volumeMount = new VolumeMount();
            volumeMount.setName("input-data");
            volumeMount.setMountPath(k8STaskMainParameters.getPodInputDataVolume());
            volumeMounts.add(volumeMount);
            //宿主机
            Volume volume = new Volume();
            volume.setName("input-data");
            volume.setHostPath(new HostPathVolumeSource(k8STaskMainParameters.getInputDataVolume() + taskInstanceId, "DirectoryOrCreate"));
            volumes.add(volume);
        }

        if (!StringUtils.isEmpty(k8STaskMainParameters.getOutputDataVolume())) {
            //容器
            VolumeMount volumeMount = new VolumeMount();
            volumeMount.setName("output-data");
            volumeMount.setMountPath(k8STaskMainParameters.getPodOutputDataVolume());
            volumeMounts.add(volumeMount);
            //宿主机
            Volume volume = new Volume();
            volume.setName("output-data");
            volume.setHostPath(new HostPathVolumeSource(k8STaskMainParameters.getOutputDataVolume() + taskInstanceId, "DirectoryOrCreate"));
            volumes.add(volume);
        }

        //组装yml文件
        QueueJob queueJob = getQueueJobTemplate();
        queueJob.setKind("Job");
        queueJob.getMetadata().setName(k8sJobName);
        queueJob.getMetadata().setLabels(labelMap);
        queueJob.getMetadata().setNamespace(namespaceName);

        QueueJobSpec queueJobSpec = queueJob.getSpec();
        queueJobSpec.setQueue(queue);
        queueJobSpec.getTasks().get(0).setName(k8sJobName);
        PodTemplateSpec template = new PodTemplateSpec();
        ObjectMeta objectMeta = new ObjectMeta();
        template.setMetadata(objectMeta);
        template.getMetadata().setLabels(podLabelMap);
        List<Container> containers = new ArrayList<>();
        Container container = new Container();
        container.setImage(image);
        container.setName(k8sJobName);
        container.setCommand(commands.size() == 0 ? null : commands);
        container.setArgs(args.size() == 0 ? null : args);
        container.setImagePullPolicy(imagePullPolicy);
        container.setResources(new ResourceRequirements(limitRes, reqRes));
        container.setEnv(envVars);
        container.setVolumeMounts(volumeMounts.size() == 0 ? null : volumeMounts);
        containers.add(container);

        PodSpec podSpec = new PodSpec();
        template.setSpec(podSpec);
        template.getSpec().setContainers(containers);
        template.getSpec().setVolumes(volumes.size() == 0 ? null : volumes);
        template.getSpec().setAffinity(affinity);
        template.getSpec().setRestartPolicy(RESTART_POLICY);


        //需要从远程拉取数据的情况下，设置Init容器初始化操作, 从接口获取挂载信息
        String fetchDataVolume = k8STaskMainParameters.getFetchDataVolume();
        if (!StringUtils.isEmpty(fetchDataVolume) && !k8STaskMainParameters.getFetchType().equals(VOLUME_LOCAL)) {
            List<String> inputArgs = new ArrayList<>();
            String fetchDataVolumeArgs = k8STaskMainParameters.getFetchDataVolumeArgs();
            try {
                if (!StringUtils.isEmpty(fetchDataVolumeArgs)) {
                    inputArgs = yaml.load(fetchDataVolumeArgs.trim());
                }
            } catch (Exception e) {
                throw new TaskException("Parse yaml-like init commands and args failed", e);
            }

            String fetchImage =
                    PropertyUtils.getString(K8S_FETCH_IMAGE);
            List<Container> initContainers = new ArrayList<>();
            Container initContainer = new Container();
            initContainer.setImage(fetchImage);
            initContainer.setImagePullPolicy("IfNotPresent");
            initContainer.setName("fetch-init");
            initContainer.setArgs(inputArgs);
            initContainer.setVolumeMounts(Lists.newArrayList(getFetchVolumeMount()));
            initContainers.add(initContainer);

            //设置初始化操作的资源
            Double podMem = 1024d;
            Double podCpu = 1d;
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            reqRes.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            reqRes.put(CPU, new Quantity(String.valueOf(podCpu)));
            limitRes.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            limitRes.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
            initContainer.setResources(new ResourceRequirements(limitRes, reqRes));
            template.getSpec().setInitContainers(initContainers);

            //新增fetch的数据到宿主机
            Volume volume = new Volume();
            volume.setName("fetch-init");
            //TODO 临时写死
            String fetchDataVolumeNode = k8STaskMainParameters.getFetchDataVolume() + taskInstanceId;
            if (taskType.equalsIgnoreCase("K8S")) {
                fetchDataVolumeNode = k8STaskMainParameters.getFetchDataVolume() + taskInstanceId + "/MNIST/raw";
            }
            volume.setHostPath(new HostPathVolumeSource(fetchDataVolumeNode, "DirectoryOrCreate"));
            List<Volume> preVolumes = template.getSpec().getVolumes();
            preVolumes.add(volume);
            template.getSpec().setVolumes(preVolumes);
        }
        queueJobSpec.getTasks().get(0).setTemplate(template);
        queueJob.setSpec(queueJobSpec);
        return queueJob;
    }

    private static QueueJob getQueueJobTemplate() {
        try {
            InputStream resourceAsStream = null;
            resourceAsStream = K8sQueueTaskExecutor.class
                    .getResourceAsStream("/queue-job.yml");
            return Serialization.yamlMapper()
                    .readValue(resourceAsStream, QueueJob.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private VolumeMount getFetchVolumeMount() {
        VolumeMount volumeMount = new VolumeMount();
        volumeMount.setName("fetch-init");
        volumeMount.setMountPath("/app/downloads");
        return volumeMount;
    }

    private void registerBatchJobWatcher(String taskInstanceId, TaskResponse taskResponse,
                                         K8sTaskMainParameters k8STaskMainParameters) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Watcher<GenericKubernetesResource> watcher = new Watcher<GenericKubernetesResource>() {
            @Override
            public void eventReceived(Action action, GenericKubernetesResource resource) {
                try {
                    LogUtils.setTaskInstanceLogFullPathMDC(taskRequest.getLogPath());
                    log.info("event received : job:{} action:{}", resource.getMetadata().getName(),
                            action);
                    if (action != Action.ADDED && action != Action.DELETED) {
                        int jobStatus = getK8sJobStatus(resource);
                        log.info("watch queue job operator :{}", jobStatus);
                        setTaskStatus(jobStatus, taskInstanceId, taskResponse,
                                k8STaskMainParameters);
                        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
                            countDownLatch.countDown();
                        }
                    } else if (action == Action.DELETED) {
                        log.error("[K8sJobExecutor-{}] fail in queue operator k8s",
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
                        job.getMetadata().getName(), e.getMessage()));
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
            watch = k8sUtils.createQueueJobWatcher(
                    job.getMetadata().getName(), job.getMetadata().getNamespace(), watcher);
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
            log.error("pytorch job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } catch (Exception e) {
            log.error("pytorch job failed in k8s: {}", e.getMessage(), e);
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } finally {
            if (watch != null) {
                watch.close();
            }
        }
    }

    private void parsePodLogOutput() {
        ExecutorService collectPodLogExecutorService = ThreadUtils
                .newSingleDaemonScheduledExecutorService("CollectPodLogOutput-thread-" + taskRequest.getTaskName());

        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String containerName = String.format("%s-%s", taskName, taskInstanceId);
        podLogOutputFuture = collectPodLogExecutorService.submit(() -> {
            try (
                    LogWatch watcher = ProcessUtils.getPodLogWatcher(taskRequest.getK8sTaskExecutionContext(),
                            taskRequest.getTaskAppId(), containerName)) {
                LogUtils.setTaskInstanceLogFullPathMDC(taskRequest.getLogPath());
                String line;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(watcher.getOutput()))) {
                    while ((line = reader.readLine()) != null) {
                        log.info("[K8S-pod-log] pod name:{},{}", containerName, line);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                LogUtils.removeTaskInstanceLogFullPathMDC();
                podLogOutputIsFinished = true;
            }
        });

        collectPodLogExecutorService.shutdown();
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
            K8sTaskExecutionContext k8sTaskExecutionContext = taskRequest.getK8sTaskExecutionContext();
            String configYaml = k8sTaskExecutionContext.getConfigYaml();
            k8sUtils.buildClient(configYaml);
            submitJob2k8s(k8sParameterStr);
            parsePodLogOutput();
            K8sTaskMainParameters k8STaskMainParameters =
                    JSONUtils.parseObject(k8sParameterStr, K8sTaskMainParameters.class);
            registerBatchJobWatcher(Integer.toString(taskInstanceId), result,
                    k8STaskMainParameters);
            if (podLogOutputFuture != null) {
                try {
                    // Wait kubernetes pod log collection finished
                    podLogOutputFuture.get();
                } catch (ExecutionException e) {
                    log.error("Handle pod log error", e);
                }
            }
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
        if (job != null) {
            stopJobOnK8s(k8sParameterStr);
        }
    }

    @Override
    public void submitJob2k8s(String k8sParameterStr) {
        int taskInstanceId = taskRequest.getTaskInstanceId();
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        K8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, K8sTaskMainParameters.class);
        try {
            log.info("[K8sJobExecutor-{}-{}] start to submit job", taskName, taskInstanceId);
            String asApplyYaml = asApplyYaml(k8STaskMainParameters);
            log.info("deploy k8s queue job yaml:{}", Serialization.asYaml(job));
            stopJobOnK8s(k8sParameterStr);
            log.info("deploy yaml:{}", asApplyYaml);
            k8sUtils.loadApplyYmlJob(asApplyYaml);
            log.info("[K8sJobExecutor-{}-{}] submitted job successfully", taskName, taskInstanceId);
        } catch (Exception e) {
            log.error("[K8sJobExecutor-{}-{}] fail to submit job", taskName, taskInstanceId);
            throw new TaskException("K8sJobExecutor fail to submit job", e);
        }
    }

    private String asApplyYaml(K8sTaskMainParameters parameters) {
        this.job = buildK8sQueueJob(parameters);
        return Serialization.asYaml(this.job);
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        K8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, K8sTaskMainParameters.class);
        String namespaceName = job.getMetadata().getNamespace();
        String jobName = job.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.queueJobExist(jobName, namespaceName))) {
                k8sUtils.deleteApplyYmlJob(asApplyYaml(k8STaskMainParameters));
            }
        } catch (Exception e) {
            log.error("[K8sJobExecutor-{}] fail to stop job", jobName);
            throw new TaskException("K8sJobExecutor fail to stop job", e);
        }
    }

    private int getK8sJobStatus(GenericKubernetesResource resource) {
        Map<String, Object> additionalProperties = resource
                .getAdditionalProperties();
        if (additionalProperties != null) {
            Map<String, Object> status = (Map<String, Object>) additionalProperties
                    .get("status");
            if (status != null) {
                Map<String, Object> applicationState = (Map<String, Object>) status
                        .get("state");
                if (applicationState != null) {
                    if (applicationState.get("phase") != null) {
                        String state = applicationState.get("phase").toString();
                        if (state.equals("Completed")) {
                            return EXIT_CODE_SUCCESS;
                        } else if (state.equals("Failed")) {
                            return EXIT_CODE_FAILURE;
                        } else {
                            return RUNNING_CODE;
                        }
                    }
                }
            }
        }
        return RUNNING_CODE;
    }

    private void setTaskStatus(int jobStatus, String taskInstanceId, TaskResponse taskResponse,
                               K8sTaskMainParameters k8STaskMainParameters) {
        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
            if (null == TaskExecutionContextCacheManager
                    .getByTaskInstanceId(Integer.valueOf(taskInstanceId))) {
                log.info(String.format("[K8sQueueJobExecutor-%s] killed",
                        job.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                log.info(String.format("[K8sQueueJobExecutor-%s] succeed in k8s",
                        job.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                        .getPodLog(job.getMetadata().getName(),
                                k8STaskMainParameters.getNamespaceName());
                log.info(String.format("[K8sQueueJobExecutor-%s] fail in k8s: %s",
                        job.getMetadata().getName(), errorMessage));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
            }
        }
    }

    public QueueJob getJob() {
        return job;
    }

    public void setJob(QueueJob job) {
        this.job = job;
    }
}
