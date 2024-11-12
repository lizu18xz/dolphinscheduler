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

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sPytorchTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJob;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJobSpec;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.Tasks;
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

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.*;

/**
 * K8sTaskExecutor used to submit k8s task to K8S
 */
public class PytorchK8sQueueTaskExecutor extends AbstractK8sTaskExecutor {

    private QueueJob job;
    protected boolean podLogOutputIsFinished = false;
    protected Future<?> podLogOutputFuture;

    public PytorchK8sQueueTaskExecutor(Logger logger, TaskExecutionContext taskRequest) {
        super(logger, taskRequest);
    }

    /**
     * 构建有队列的任务
     */
    public QueueJob buildK8sQueueJob(K8sPytorchTaskMainParameters parameters) {
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String image = parameters.getImage();
        String namespaceName = parameters.getNamespaceName();
        String imagePullPolicy = parameters.getImagePullPolicy();
        Map<String, String> otherParams = parameters.getParamsMap();
        String queue = parameters.getQueue() == null ? "default" : parameters.getQueue();

        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        //设置资源
        Map<String, Quantity> masterLimitRes = new HashMap<>();
        Map<String, Quantity> masterReqRes = new HashMap<>();
        // 设置资源信息
        Double masterGpuLimits = parameters.getMasterGpuLimits();
        if (masterGpuLimits != null && masterGpuLimits > 0) {
            String gpuType = parameters.getGpuType();
            if (StringUtils.isEmpty(gpuType)) {
                gpuType = GPU;
            }
            masterLimitRes.put(gpuType,
                    new Quantity(parameters.getMasterGpuLimits() == null ? "1" : String.valueOf(parameters.getMasterGpuLimits())));
        } else {
            // 资源设置
            Double podMem = parameters.getMasterMinMemorySpace();
            Double podCpu = parameters.getMasterMinCpuCores();
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            masterReqRes.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            masterReqRes.put(CPU, new Quantity(String.valueOf(podCpu)));
            masterLimitRes.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            masterLimitRes.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
        }

        Map<String, Quantity> workerLimitRes = new HashMap<>();
        Map<String, Quantity> workerReqRes = new HashMap<>();

        Double workerGpuLimits = parameters.getWorkerGpuLimits();
        if (workerGpuLimits != null && workerGpuLimits > 0) {
            String gpuType = parameters.getGpuType();
            if (StringUtils.isEmpty(gpuType)) {
                gpuType = GPU;
            }
            workerLimitRes.put(gpuType,
                    new Quantity(parameters.getWorkerGpuLimits() == null ? "1" : String.valueOf(parameters.getWorkerGpuLimits())));
        } else {
            Double podMem = parameters.getWorkerMinMemorySpace();
            Double podCpu = parameters.getWorkerMinCpuCores();
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            workerReqRes.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            workerReqRes.put(CPU, new Quantity(String.valueOf(podCpu)));
            workerLimitRes.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            workerLimitRes.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
        }

        Map<String, String> labelMap = parameters.getLabelMap();
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

        String commandString = parameters.getCommand();
        String argsString = parameters.getArgs();
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
        nodeSelectorTerm.setMatchExpressions(parameters.getNodeSelectorRequirements());

        Affinity affinity = parameters.getNodeSelectorRequirements().size() == 0 ? null
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
        if (!StringUtils.isEmpty(parameters.getInputDataVolume())) {
            //容器
            VolumeMount volumeMount = new VolumeMount();
            volumeMount.setName("input-data");
            volumeMount.setMountPath(parameters.getPodInputDataVolume());
            volumeMounts.add(volumeMount);
            //宿主机
            Volume volume = new Volume();
            volume.setName("input-data");
            volume.setHostPath(new HostPathVolumeSource(parameters.getInputDataVolume(), "DirectoryOrCreate"));
            volumes.add(volume);
        }

        if (!StringUtils.isEmpty(parameters.getOutputDataVolume())) {
            //容器
            VolumeMount volumeMount = new VolumeMount();
            volumeMount.setName("output-data");
            volumeMount.setMountPath(parameters.getPodOutputDataVolume());
            volumeMounts.add(volumeMount);
            //宿主机
            Volume volume = new Volume();
            volume.setName("output-data");
            volume.setHostPath(new HostPathVolumeSource(parameters.getOutputDataVolume(), "DirectoryOrCreate"));
            volumes.add(volume);
        }

        //组装yml文件
        QueueJob queueJob = getQueueJobTemplate();
        queueJob.getMetadata().setName(k8sJobName);
        queueJob.getMetadata().setLabels(labelMap);
        queueJob.getMetadata().setNamespace(namespaceName);
        QueueJobSpec queueJobSpec = queueJob.getSpec();
        queueJobSpec.setQueue(queue);

        //设置master
        Tasks master = queueJobSpec.getTasks().get(0);
        master.setName("master");
        master.setReplicas(parameters.getMasterReplicas() == null ? 1 : parameters.getMasterReplicas());
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
        container.setResources(new ResourceRequirements(masterLimitRes, masterReqRes));
        container.setEnv(envVars);
        container.setVolumeMounts(volumeMounts.size() == 0 ? null : volumeMounts);
        containers.add(container);

        PodSpec podSpec = new PodSpec();
        template.setSpec(podSpec);
        template.getSpec().setContainers(containers);
        template.getSpec().setVolumes(volumes.size() == 0 ? null : volumes);
        template.getSpec().setAffinity(affinity);
        template.getSpec().setRestartPolicy(RESTART_POLICY);
        //设置拉取镜像权限
        List<LocalObjectReference> imagePullSecrets = new ArrayList<>();
        LocalObjectReference reference = new LocalObjectReference();
        reference.setName(DOLPHIN_HARBOR);
        imagePullSecrets.add(reference);
        template.getSpec().setImagePullSecrets(imagePullSecrets);
        master.setTemplate(template);

        //设置worker
        Tasks worker = queueJobSpec.getTasks().get(1);
        worker.setName("worker");
        worker.setReplicas(parameters.getWorkerReplicas() == null ? 1 : parameters.getWorkerReplicas());
        PodTemplateSpec workerTemplate = new PodTemplateSpec();

        ObjectMeta workerObjectMeta = new ObjectMeta();
        workerTemplate.setMetadata(workerObjectMeta);
        workerTemplate.getMetadata().setLabels(podLabelMap);
        List<Container> workerContainers = new ArrayList<>();
        Container workerContainer = new Container();
        workerContainer.setImage(image);
        workerContainer.setName(k8sJobName);
        workerContainer.setCommand(commands.size() == 0 ? null : commands);
        workerContainer.setArgs(args.size() == 0 ? null : args);
        workerContainer.setImagePullPolicy(imagePullPolicy);
        workerContainer.setResources(new ResourceRequirements(workerLimitRes, workerReqRes));
        workerContainer.setEnv(envVars);
        workerContainer.setVolumeMounts(volumeMounts.size() == 0 ? null : volumeMounts);
        workerContainers.add(container);

        PodSpec workerPodSpec = new PodSpec();
        workerTemplate.setSpec(workerPodSpec);
        workerTemplate.getSpec().setContainers(workerContainers);
        workerTemplate.getSpec().setVolumes(volumes.size() == 0 ? null : volumes);
        workerTemplate.getSpec().setAffinity(affinity);
        workerTemplate.getSpec().setRestartPolicy(RESTART_POLICY);
        //设置拉取镜像权限
        workerTemplate.getSpec().setImagePullSecrets(imagePullSecrets);
        worker.setTemplate(workerTemplate);
        queueJob.setSpec(queueJobSpec);
        return queueJob;
    }

    private static QueueJob getQueueJobTemplate() {
        try {
            InputStream resourceAsStream = null;
            resourceAsStream = PytorchK8sQueueTaskExecutor.class
                    .getResourceAsStream("/pytorch-job.yaml");
            return Serialization.yamlMapper()
                    .readValue(resourceAsStream, QueueJob.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
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
                        log.info("watch queue pytorch job operator :{}", jobStatus);
                        setTaskStatus(jobStatus, taskInstanceId, taskResponse,
                                k8STaskMainParameters);
                        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
                            countDownLatch.countDown();
                        }
                    } else if (action == Action.DELETED) {
                        log.error("[K8sPytorchJobExecutor-{}] fail in queue operator k8s",
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
                log.info(String.format("[K8sPytorchJobExecutor-%s] fail in k8s: %s",
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
                        log.info("[K8s pytorchJob-pod-log] {}", line);
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
        K8sPytorchTaskMainParameters k8sPytorchTaskMainParameters = JSONUtils
                .parseObject(k8sParameterStr, K8sPytorchTaskMainParameters.class);
        try {
            log.info("[K8sPytorchJobExecutor-{}-{}] start to submit job", taskName, taskInstanceId);
            String asApplyYaml = asApplyYaml(k8sPytorchTaskMainParameters);
            log.info("deploy k8s pytorch queue job yaml:{}", Serialization.asYaml(job));
            stopJobOnK8s(k8sParameterStr);
            log.info("deploy yaml:{}", asApplyYaml);
            k8sUtils.loadApplyYmlJob(asApplyYaml);
            log.info("[K8sJobExecutor-{}-{}] submitted job successfully", taskName, taskInstanceId);
        } catch (Exception e) {
            log.error("[K8sJobExecutor-{}-{}] fail to submit job", taskName, taskInstanceId);
            throw new TaskException("K8sJobExecutor fail to submit job", e);
        }
    }

    private String asApplyYaml(K8sPytorchTaskMainParameters parameters) {
        this.job = buildK8sQueueJob(parameters);
        return Serialization.asYaml(this.job);
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        K8sPytorchTaskMainParameters parameters = JSONUtils
                .parseObject(k8sParameterStr, K8sPytorchTaskMainParameters.class);
        String namespaceName = job.getMetadata().getNamespace();
        String jobName = job.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.queueJobExist(jobName, namespaceName))) {
                k8sUtils.deleteApplyYmlJob(asApplyYaml(parameters));
            }
        } catch (Exception e) {
            log.error("[K8sPytorchJobExecutor-{}] fail to stop job", jobName);
            throw new TaskException("K8sPytorchJobExecutor fail to stop job", e);
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
                log.info(String.format("[K8sPytorchJobExecutor-%s] killed",
                        job.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                log.info(String.format("[K8sPytorchJobExecutor-%s] succeed in k8s",
                        job.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                        .getPodLog(job.getMetadata().getName(),
                                k8STaskMainParameters.getNamespaceName());
                log.info(String.format("[K8sPytorchJobExecutor-%s] fail in k8s: %s",
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
