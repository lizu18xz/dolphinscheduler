package org.apache.dolphinscheduler.plugin.task.api.k8s.impl;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sPytorchTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator.PyTorchJob;
import org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator.PytorchReplicaSpecs;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;

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

import org.slf4j.Logger;
import org.springframework.util.CollectionUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.utils.Serialization;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.*;

/**
 * @author lizu
 * @since 2024/8/13
 */
public class PytorchK8sOperatorTaskExecutor extends AbstractK8sTaskExecutor {

    private PyTorchJob pyTorchJob;

    private K8sTaskExecutionContext k8sTaskExecutionContext;

    public PytorchK8sOperatorTaskExecutor(Logger logger, TaskExecutionContext taskRequest) {
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
            // 日志
            K8sPytorchTaskMainParameters k8sPytorchTaskMainParameters = JSONUtils
                    .parseObject(k8sParameterStr, K8sPytorchTaskMainParameters.class);
            registerBatchJobWatcher(Integer.toString(taskInstanceId), result,
                    k8sPytorchTaskMainParameters);
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
        if (pyTorchJob != null) {
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
            log.info("[K8sOperatorJobExecutor-{}-{}] start to submit Pytorch job", taskName,
                    taskInstanceId);
            String asApplyYaml = asApplyYaml(k8sPytorchTaskMainParameters);
            stopJobOnK8s(k8sParameterStr);
            log.info("deploy yaml:{}", asApplyYaml);
            k8sUtils.loadApplyYmlJob(asApplyYaml);
            log.info("[K8sOperatorJobExecutor-{}-{}]  submitted Pytorch job successfully", taskName,
                    taskInstanceId);
        } catch (Exception e) {
            log.error("[K8sOperatorJobExecutor-{}-{}]  fail to submit Pytorch job", taskName,
                    taskInstanceId);
            throw new TaskException("K8sOperatorJobExecutor fail to submit Pytorch job", e);
        }
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        String namespaceName = pyTorchJob.getMetadata().getNamespace();
        String jobName = pyTorchJob.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.pytorchJobExist(jobName, namespaceName))) {
                K8sPytorchTaskMainParameters parameters = JSONUtils
                        .parseObject(k8sParameterStr, K8sPytorchTaskMainParameters.class);
                k8sUtils.deleteApplyYmlJob(asApplyYaml(parameters));
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("[K8sJobExecutor-{}]  fail to stop Pytorch job", jobName);
            throw new TaskException("K8sJobExecutor fail to stop Pytorch job", e);
        }
    }

    private void registerBatchJobWatcher(String taskInstanceId, TaskResponse taskResponse,
                                         K8sPytorchTaskMainParameters k8sPytorchTaskMainParameters) {
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
                        log.info("watch pytorch operator :{}", jobStatus);
                        setTaskStatus(jobStatus, taskInstanceId, taskResponse,
                                k8sPytorchTaskMainParameters);
                        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
                            countDownLatch.countDown();
                        }
                    } else if (action == Action.DELETED) {
                        log.error("[K8sJobExecutor-{}] fail in pytorch operator k8s",
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
                        pyTorchJob.getMetadata().getName(), e.getMessage()));
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
            watch = k8sUtils.createPytorchJobWatcher(
                    pyTorchJob.getMetadata().getName(), pyTorchJob.getMetadata().getNamespace(), watcher);
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

    /**
     * Kind类型
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
                        if (state.equals("FINISHED")) {
                            return EXIT_CODE_SUCCESS;
                        } else if (state.equals("FAILED")) {
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
                               K8sPytorchTaskMainParameters k8STaskMainParameters) {
        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
            if (null == TaskExecutionContextCacheManager
                    .getByTaskInstanceId(Integer.valueOf(taskInstanceId))) {
                log.info(String.format("[K8sPytorchOperatorJobExecutor-%s] killed",
                        pyTorchJob.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                log.info(String.format("[K8sPytorchOperatorJobExecutor-%s] succeed in k8s",
                        pyTorchJob.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                        .getPodLog(pyTorchJob.getMetadata().getName(),
                                k8STaskMainParameters.getNamespaceName());
                log.info(String.format("[K8sPytorchOperatorJobExecutor-%s] fail in k8s: %s",
                        pyTorchJob.getMetadata().getName(), errorMessage));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
            }
        }
    }

    private String asApplyYaml(K8sPytorchTaskMainParameters parameters) {
        PyTorchJob pyTorchJob = buildPyTorchJob(parameters);
        return Serialization.asYaml(pyTorchJob);
    }

    private PyTorchJob buildPyTorchJob(K8sPytorchTaskMainParameters parameters) {
        // 获取模版
        this.pyTorchJob = getPyTorchJob();

        // 新增填充 设置job名称
        String k8sJobName = getK8sJobName(parameters);
        this.pyTorchJob.getMetadata().setName(k8sJobName);
        this.pyTorchJob.getMetadata()
                .setNamespace(k8sTaskExecutionContext.getNamespace().replace("_", ""));

        // 设置Master、Worker信息
        PytorchReplicaSpecs pytorchReplicaSpecs = this.pyTorchJob.getSpec().getPytorchReplicaSpecs();
        pytorchReplicaSpecs.getMaster()
                .setReplicas(parameters.getMasterReplicas() == null ? 1 : parameters.getMasterReplicas());
        pytorchReplicaSpecs.getWorker()
                .setReplicas(parameters.getWorkerReplicas() == null ? 1 : parameters.getWorkerReplicas());

        Container containerMaster = new Container();
        Container containerWorker = new Container();

        PodTemplateSpec masterTemplate = pytorchReplicaSpecs.getMaster().getTemplate();
        containerMaster.setName("pytorch");
        containerMaster.setImage(parameters.getImage());
        containerMaster.setImagePullPolicy(parameters.getImagePullPolicy());

        // 设置资源信息
        String masterGpuLimits = parameters.getMasterGpuLimits();
        if (!StringUtils.isEmpty(masterGpuLimits)) {
            ResourceRequirements masterRes = new ResourceRequirements();
            Map<String, Quantity> limits = new HashMap<>();
            limits.put(GPU,
                    new Quantity(parameters.getMasterGpuLimits() == null ? "1" : parameters.getMasterGpuLimits()));
            masterRes.setLimits(limits);
            containerMaster.setResources(masterRes);
        } else {
            // 资源设置
            ResourceRequirements masterRes = new ResourceRequirements();
            Map<String, Quantity> requests = new HashMap<>();
            Map<String, Quantity> limits = new HashMap<>();
            Double podMem = parameters.getMasterMinMemorySpace();
            Double podCpu = parameters.getMasterMinCpuCores();
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            requests.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            requests.put(CPU, new Quantity(String.valueOf(podCpu)));
            limits.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            limits.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
            masterRes.setRequests(requests);
            masterRes.setLimits(limits);
            containerMaster.setResources(masterRes);
        }

        String workerGpuLimits = parameters.getWorkerGpuLimits();
        if (!StringUtils.isEmpty(workerGpuLimits)) {
            ResourceRequirements workerRes = new ResourceRequirements();
            Map<String, Quantity> workerLimits = new HashMap<>();
            workerLimits.put(GPU,
                    new Quantity(parameters.getWorkerGpuLimits() == null ? "1" : parameters.getWorkerGpuLimits()));
            workerRes.setLimits(workerLimits);
            containerWorker.setResources(workerRes);
        } else {
            ResourceRequirements workerRes = new ResourceRequirements();
            Map<String, Quantity> workerRequests = new HashMap<>();
            Map<String, Quantity> workerLimits = new HashMap<>();
            Double podMem = parameters.getMasterMinMemorySpace();
            Double podCpu = parameters.getMasterMinCpuCores();
            Double limitPodMem = podMem * 2;
            Double limitPodCpu = podCpu * 2;
            workerRequests.put(MEMORY, new Quantity(String.format("%s%s", podMem, MI)));
            workerRequests.put(CPU, new Quantity(String.valueOf(podCpu)));
            workerLimits.put(MEMORY, new Quantity(String.format("%s%s", limitPodMem, MI)));
            workerLimits.put(CPU, new Quantity(String.valueOf(limitPodCpu)));
            workerRes.setRequests(workerRequests);
            workerRes.setLimits(workerLimits);
            containerWorker.setResources(workerRes);
        }

        // 设置镜像启动命令
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

        containerMaster.setCommand(commands.size() == 0 ? null : commands);
        containerMaster.setArgs(args.size() == 0 ? null : args);
        masterTemplate.getSpec().setContainers(Arrays.asList(containerMaster));

        // 设置Worker的信息
        PodTemplateSpec workerTemplate = pytorchReplicaSpecs.getWorker().getTemplate();
        containerWorker.setName("pytorch");
        containerWorker.setImage(parameters.getImage());
        containerWorker.setImagePullPolicy(parameters.getImagePullPolicy());
        containerWorker.setCommand(commands.size() == 0 ? null : commands);
        containerWorker.setArgs(args.size() == 0 ? null : args);
        workerTemplate.getSpec().setContainers(Arrays.asList(containerWorker));

        // 设置label
        Map<String, String> podLabelMap = new HashMap<>();
        podLabelMap.put(UNIQUE_LABEL_NAME, taskRequest.getTaskAppId());
        ObjectMeta masterObjectMeta = new ObjectMeta();
        masterObjectMeta.setLabels(podLabelMap);
        this.pyTorchJob.getSpec().getPytorchReplicaSpecs().getMaster().getTemplate().setMetadata(masterObjectMeta);

        ObjectMeta workerObjectMeta = new ObjectMeta();
        workerObjectMeta.setLabels(podLabelMap);
        this.pyTorchJob.getSpec().getPytorchReplicaSpecs().getWorker().getTemplate().setMetadata(workerObjectMeta);

        // 设置环境变量
        if (!CollectionUtils.isEmpty(parameters.getParamsMap())) {
            containerMaster = pytorchReplicaSpecs.getMaster().getTemplate().getSpec()
                    .getContainers().get(0);
            containerMaster.setEnv(getEnv(parameters.getParamsMap()));
            pytorchReplicaSpecs.getMaster().getTemplate().getSpec()
                    .setContainers(Arrays.asList(containerMaster));

            containerWorker = pytorchReplicaSpecs.getWorker().getTemplate().getSpec()
                    .getContainers().get(0);
            containerWorker.setEnv(getEnv(parameters.getParamsMap()));
            pytorchReplicaSpecs.getWorker().getTemplate().getSpec()
                    .setContainers(Arrays.asList(containerMaster));
        }
        // TODO 设置挂载


        return pyTorchJob;
    }

    /**
     * 根据模板文件获取
     */
    private static PyTorchJob getPyTorchJob() {
        try {
            InputStream resourceAsStream = null;
            resourceAsStream = PytorchK8sOperatorTaskExecutor.class
                    .getResourceAsStream("/pytorch-on-k8s-operator.yaml");
            return Serialization.yamlMapper()
                    .readValue(resourceAsStream, PyTorchJob.class);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private String getK8sJobName(K8sPytorchTaskMainParameters parameters) {
        // 设置job名称
        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        return k8sJobName;
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
