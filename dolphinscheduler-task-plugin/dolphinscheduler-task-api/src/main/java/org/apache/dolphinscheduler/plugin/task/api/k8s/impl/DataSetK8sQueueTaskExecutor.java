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
import org.apache.dolphinscheduler.plugin.task.api.k8s.DataSetK8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJob;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJobSpec;
import org.apache.dolphinscheduler.plugin.task.api.model.FetchInfo;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.HttpRequestUtil;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.MapUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.ProcessUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.dolphinscheduler.common.constants.Constants.K8S_FETCH_IMAGE;
import static org.apache.dolphinscheduler.common.constants.Constants.TASK_DATASET_ADDRESS;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.*;

/**
 * K8sTaskExecutor used to submit k8s task to K8S
 */
public class DataSetK8sQueueTaskExecutor extends AbstractK8sTaskExecutor {

    private QueueJob job;
    protected boolean podLogOutputIsFinished = false;
    protected Future<?> podLogOutputFuture;

    private List<QueueJob> batchJobs;

    public DataSetK8sQueueTaskExecutor(Logger logger, TaskExecutionContext taskRequest) {
        super(logger, taskRequest);
        this.batchJobs = new ArrayList<>();
    }

    private String getK8sJobName(Integer index) {
        // 设置job名称
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT).replaceAll("_", "");
        String k8sJobName = "";
        if (index == null) {
            k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        } else {
            k8sJobName = String.format("%s-%s-%s", taskName, taskInstanceId, index);
        }
        return k8sJobName;
    }

    /**
     * 构建有队列的任务
     */
    public QueueJob buildK8sQueueJob(DataSetK8sTaskMainParameters k8STaskMainParameters, Integer index) {
        //目录加上索引，区分
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String image = k8STaskMainParameters.getImage();
        String namespaceName = k8STaskMainParameters.getNamespaceName();
        String imagePullPolicy = k8STaskMainParameters.getImagePullPolicy();
        Map<String, String> otherParams = k8STaskMainParameters.getParamsMap();
        String queue = k8STaskMainParameters.getQueue() == null ? "default" : k8STaskMainParameters.getQueue();
        //设置资源
        Map<String, Quantity> limitRes = new HashMap<>();
        Map<String, Quantity> reqRes = new HashMap<>();
        String k8sJobName = getK8sJobName(index);
        if (k8STaskMainParameters.getGpuLimits() == null || k8STaskMainParameters.getGpuLimits() <= 0) {
            Double podMem = k8STaskMainParameters.getMinMemorySpace();
            Double podCpu = k8STaskMainParameters.getMinCpuCores();
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            reqRes.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            reqRes.put(CPU, new Quantity(String.valueOf(podCpu)));
            limitRes.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            limitRes.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
            log.info("cpu param set :{},{}", podMem, podCpu);
        } else {
            //nvidia.com/gpu: 1
            String gpuType = k8STaskMainParameters.getGpuType();
            Double podGpu = k8STaskMainParameters.getGpuLimits();
            if (StringUtils.isEmpty(gpuType)) {
                gpuType = GPU;
            }
            log.info("gpu param set :{}", gpuType, podGpu);
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

        Yaml yaml = new Yaml();
        try {
            if (!StringUtils.isEmpty(commandString)) {
                log.info("commandString:{}", commandString);
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

        String volumeSuffix = "";
        if (index == null) {
            volumeSuffix = "";
        } else {
            volumeSuffix = "/" + index;
        }
        //设置容器挂载
        List<VolumeMount> volumeMounts = new ArrayList<>();
        //设置宿主机挂载
        List<Volume> volumes = new ArrayList<>();
        //有数据来源的情况
        if (!CollectionUtils.isEmpty(k8STaskMainParameters.getFetchInfos())) {
            //容器
            VolumeMount volumeMount = new VolumeMount();
            volumeMount.setName("input-data");
            volumeMount.setMountPath(k8STaskMainParameters.getPodInputDataVolume());
            volumeMounts.add(volumeMount);
            //宿主机
            Volume volume = new Volume();
            volume.setName("input-data");
            volume.setHostPath(new HostPathVolumeSource(k8STaskMainParameters.getInputDataVolume() + taskInstanceId + volumeSuffix, "DirectoryOrCreate"));
            volumes.add(volume);
        } else {
            //没有数据来源的节点从前置节点中获取
            if (!StringUtils.isEmpty(k8STaskMainParameters.getInputDataVolume())) {
                VolumeMount volumeMount = new VolumeMount();
                volumeMount.setName("input-data");
                volumeMount.setMountPath(k8STaskMainParameters.getPodInputDataVolume());
                volumeMounts.add(volumeMount);
                //宿主机
                Volume volume = new Volume();
                volume.setName("input-data");
                volume.setHostPath(new HostPathVolumeSource(k8STaskMainParameters.getInputDataVolume(), "DirectoryOrCreate"));
                volumes.add(volume);
            }
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
            volume.setHostPath(new HostPathVolumeSource(k8STaskMainParameters.getOutputDataVolume() + taskInstanceId + volumeSuffix, "DirectoryOrCreate"));
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
        //设置拉取镜像权限
        List<LocalObjectReference> imagePullSecrets = new ArrayList<>();
        LocalObjectReference reference = new LocalObjectReference();
        reference.setName("registry-harbor");
        imagePullSecrets.add(reference);
        template.getSpec().setImagePullSecrets(imagePullSecrets);
        template.getSpec().setVolumes(volumes.size() == 0 ? null : volumes);
        template.getSpec().setAffinity(affinity);
        template.getSpec().setRestartPolicy(RESTART_POLICY);

        //需要从远程拉取数据的情况下，设置Init容器初始化操作, 从接口获取挂载信息
        List<FetchInfo> fetchInfos = k8STaskMainParameters.getFetchInfos();
        if (!CollectionUtils.isEmpty(fetchInfos)) {
            if (index == null) {
                index = 0;
            }
            FetchInfo fetchInfo = fetchInfos.get(index);
            List<String> inputArgs = new ArrayList<>();
            String fetchDataVolumeArgs = fetchInfo.getFetchDataVolumeArgs();
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
            Map<String, Quantity> initReqRes = new HashMap<>();
            Map<String, Quantity> initLimitRes = new HashMap<>();
            initReqRes.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            initReqRes.put(CPU, new Quantity(String.valueOf(podCpu)));
            initLimitRes.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            initLimitRes.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
            initContainer.setResources(new ResourceRequirements(initLimitRes, initReqRes));
            template.getSpec().setInitContainers(initContainers);

            //新增fetch的数据到宿主机
            Volume volume = new Volume();
            volume.setName("fetch-init");

            String fetchDataVolumeNode = fetchInfo.getFetchDataVolume() + taskInstanceId + volumeSuffix;
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
            resourceAsStream = DataSetK8sQueueTaskExecutor.class
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
                                         DataSetK8sTaskMainParameters k8STaskMainParameters) {
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
            log.error("data set job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } catch (Exception e) {
            log.error("data set job failed in k8s: {}", e.getMessage(), e);
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } finally {
            if (watch != null) {
                watch.close();
            }
        }
    }


    private void registerSingleBatchJobWatcher(String taskInstanceId, TaskResponse taskResponse,
                                               DataSetK8sTaskMainParameters k8STaskMainParameters, Integer index, QueueJob queueJob) {
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
                        log.info("watch queue job operator :{},job index:{}", jobStatus, index);
                        setBatchTaskStatus(jobStatus, taskInstanceId, taskResponse,
                                k8STaskMainParameters, queueJob);
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
                        queueJob.getMetadata().getName(), e.getMessage()));
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
                    queueJob.getMetadata().getName(), queueJob.getMetadata().getNamespace(), watcher);
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
            log.error("data set job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } catch (Exception e) {
            log.error("data set job failed in k8s: {}", e.getMessage(), e);
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
        log.info("pod log name:{}", containerName);
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
        //批量执行任务
        TaskResponse result = new TaskResponse();

        int taskInstanceId = taskRequest.getTaskInstanceId();
        DataSetK8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, DataSetK8sTaskMainParameters.class);
        K8sTaskExecutionContext k8sTaskExecutionContext = taskRequest.getK8sTaskExecutionContext();
        String configYaml = k8sTaskExecutionContext.getConfigYaml();
        k8sUtils.buildClient(configYaml);
        //根据选择的数据来源来进行任务的执行
        List<FetchInfo> fetchInfos = k8STaskMainParameters.getFetchInfos();
        Boolean multiple = k8STaskMainParameters.getMultiple();
        if (multiple == null) {
            multiple = false;
        }
        if (multiple) {
            log.info("多pod 执行");
            if (null == TaskExecutionContextCacheManager.getByTaskInstanceId(taskInstanceId)) {
                result.setExitStatusCode(EXIT_CODE_KILL);
                return result;
            }
            if (StringUtils.isEmpty(k8sParameterStr)) {
                TaskExecutionContextCacheManager.removeByTaskInstanceId(taskInstanceId);
                return result;
            }

            doBatchSubmitJob2K8s(fetchInfos, k8sParameterStr, taskInstanceId, k8STaskMainParameters, result);

        } else {
            log.info("单pod 执行");
            if (null == TaskExecutionContextCacheManager.getByTaskInstanceId(taskInstanceId)) {
                result.setExitStatusCode(EXIT_CODE_KILL);
                return result;
            }
            if (StringUtils.isEmpty(k8sParameterStr)) {
                TaskExecutionContextCacheManager.removeByTaskInstanceId(taskInstanceId);
                return result;
            }
            try {
                submitJob2k8s(k8sParameterStr);
                parsePodLogOutput();
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
        }
        return result;
    }

    private void doBatchSubmitJob2K8s(List<FetchInfo> fetchInfos, String k8sParameterStr,
                                      int taskInstanceId, DataSetK8sTaskMainParameters k8STaskMainParameters, TaskResponse taskResponse) {
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        List<SubmitJob> submitJobs = new ArrayList<>();
        for (int i = 0; i < fetchInfos.size(); i++) {
            submitJobs.add(new SubmitJob(i, k8sParameterStr, taskInstanceId, k8STaskMainParameters));
        }
        log.info("start batch job size:{}", submitJobs.size());
        //批量执行
        try {
            List<Future<TaskResponse>> result = executorService.invokeAll(submitJobs);
            List<String> dataSetInfos = new ArrayList<>();
            for (Future future : result) {
                TaskResponse response = (TaskResponse) future.get();
                //处理所有的返回,获取每个任务成功失败情况
                int exitStatusCode = response.getExitStatusCode();
                dataSetInfos.add(response.getResultString() + "," + exitStatusCode);
                log.info("exitStatusCode:{}", response.getResultString() + "," + exitStatusCode);
            }
            saveDataSetInfo(dataSetInfos);
            //设置成功
            taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
        } catch (Exception e) {
            log.error("批量invoke all error:{}", e);
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }
    }

    private void saveDataSetInfo(List<String> dataSetInfo) {
        try {
            int processInstanceId = taskRequest.getProcessInstanceId();
            String address =
                    PropertyUtils.getString(TASK_DATASET_ADDRESS);
            if (StringUtils.isEmpty(address)) {
                throw new IllegalArgumentException("task.upload.address not found");
            }
            List<Map<String, Object>> lists = new ArrayList<>();
            Map<String, Object> outputInfoMap = new HashMap<>();
            for (String info : dataSetInfo) {
                //index + "," + fetchInfo.getFetchName() + "," + taskInstanceId
                String[] split = info.split(",");
                outputInfoMap.put("name", split[1]);
                outputInfoMap.put("taskInstanceId", split[2]);
                outputInfoMap.put("status", split[3]);
                outputInfoMap.put("processInstanceId", processInstanceId);
                lists.add(outputInfoMap);
            }
            HttpPost httpPost = HttpRequestUtil.constructHttpPost(address, JSONUtils.toJsonString(lists));
            CloseableHttpClient httpClient = HttpRequestUtil.getHttpClient();
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

        } catch (Exception e) {
            log.info("save data set info error:{}", e);
        }
    }

    class SubmitJob implements Callable<TaskResponse> {
        /**
         * 执行任务的索引
         */
        private Integer index;

        private String k8sParameterStr;

        private int taskInstanceId;

        private DataSetK8sTaskMainParameters k8STaskMainParameters;

        public SubmitJob(Integer index, String k8sParameterStr,
                         int taskInstanceId, DataSetK8sTaskMainParameters k8STaskMainParameters) {
            this.index = index;
            this.k8sParameterStr = k8sParameterStr;
            this.taskInstanceId = taskInstanceId;
            this.k8STaskMainParameters = k8STaskMainParameters;
        }

        @Override
        public TaskResponse call() throws Exception {
            TaskResponse result = new TaskResponse();
            try {
                QueueJob queueJob = submitBatchJob2k8s(k8sParameterStr, index);
                batchJobs.add(queueJob);
                registerSingleBatchJobWatcher(Integer.toString(taskInstanceId), result,
                        k8STaskMainParameters, index, queueJob);
            } catch (Exception e) {
                result.setExitStatusCode(EXIT_CODE_FAILURE);
                cancelApplication(k8sParameterStr);
                log.info("批量任务中单个任务异常:{}", e);
            }
            //记录文件名称等信息
            List<FetchInfo> fetchInfos = k8STaskMainParameters.getFetchInfos();
            FetchInfo fetchInfo = fetchInfos.get(index);
            result.setResultString(index + "," + fetchInfo.getFetchName() + "," + taskInstanceId);
            return result;
        }
    }

    @Override
    public void cancelApplication(String k8sParameterStr) {
        if (job != null) {
            stopJobOnK8s(k8sParameterStr);
        }
        if (!CollectionUtils.isEmpty(batchJobs)) {
            for (int i = 0; i < batchJobs.size(); i++) {
                stopBatchJobOnK8s(k8sParameterStr, batchJobs.get(i), i);
            }
        }
    }

    public QueueJob submitBatchJob2k8s(String k8sParameterStr, Integer index) {
        int taskInstanceId = taskRequest.getTaskInstanceId();
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        DataSetK8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, DataSetK8sTaskMainParameters.class);
        try {
            log.info("[K8sJobExecutor-{}-{}--{}] start to submit job", taskName, taskInstanceId, index);
            QueueJob queueJob = asApplyYaml(k8STaskMainParameters, index);
            String asApplyYaml = Serialization.asYaml(queueJob);
            log.info("deploy k8s queue job yaml:{}", Serialization.asYaml(job));
            stopBatchJobOnK8s(k8sParameterStr, queueJob, index);
            log.info("deploy yaml:{}", asApplyYaml);
            k8sUtils.loadApplyYmlJob(asApplyYaml);
            log.info("[K8sJobExecutor-{}-{}-{}] submitted job successfully", taskName, taskInstanceId, index);
            return queueJob;
        } catch (Exception e) {
            log.error("[K8sJobExecutor-{}-{}-{}] fail to submit job", taskName, taskInstanceId, index);
            throw new TaskException("K8sJobExecutor fail to submit job", e);
        }
    }

    @Override
    public void submitJob2k8s(String k8sParameterStr) {
        int taskInstanceId = taskRequest.getTaskInstanceId();
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        DataSetK8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, DataSetK8sTaskMainParameters.class);
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

    private String asApplyYaml(DataSetK8sTaskMainParameters parameters) {
        this.job = buildK8sQueueJob(parameters, null);
        return Serialization.asYaml(this.job);
    }

    private QueueJob asApplyYaml(DataSetK8sTaskMainParameters parameters, Integer index) {
        return buildK8sQueueJob(parameters, index);
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        DataSetK8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, DataSetK8sTaskMainParameters.class);
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

    public void stopBatchJobOnK8s(String k8sParameterStr, QueueJob job, Integer index) {
        DataSetK8sTaskMainParameters k8STaskMainParameters =
                JSONUtils.parseObject(k8sParameterStr, DataSetK8sTaskMainParameters.class);
        String namespaceName = job.getMetadata().getNamespace();
        String jobName = job.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.queueJobExist(jobName, namespaceName))) {
                k8sUtils.deleteApplyYmlJob(Serialization.asYaml(asApplyYaml(k8STaskMainParameters, index)));
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
                               DataSetK8sTaskMainParameters k8STaskMainParameters) {
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

    private void setBatchTaskStatus(int jobStatus, String taskInstanceId, TaskResponse taskResponse,
                                    DataSetK8sTaskMainParameters k8STaskMainParameters, QueueJob queueJob) {
        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
            if (null == TaskExecutionContextCacheManager
                    .getByTaskInstanceId(Integer.valueOf(taskInstanceId))) {
                log.info(String.format("[K8sQueueJobExecutor-%s] killed",
                        queueJob.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                log.info(String.format("[K8sQueueJobExecutor-%s] succeed in k8s",
                        queueJob.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                        .getPodLog(queueJob.getMetadata().getName(),
                                k8STaskMainParameters.getNamespaceName());
                log.info(String.format("[K8sQueueJobExecutor-%s] fail in k8s: %s",
                        queueJob.getMetadata().getName(), errorMessage));
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
