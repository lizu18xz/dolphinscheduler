package org.apache.dolphinscheduler.plugin.task.api.k8s.impl;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_FAILURE;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_SUCCESS;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_K8S_OPERATOR_APIVERSION;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_K8S_OPERATOR_COMPLETED;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_K8S_OPERATOR_FAILED;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_K8S_OPERATOR_KIND;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_K8S_OPERATOR_MODEL;

import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sSparkOperatorTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver.Labels;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver.VolumeMounts;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Executor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.RestartPolicy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.SparkGenericKubernetesResource;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.SparkOperatorSpec;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Volume;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Volume.HostPath;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.StringUtils;
import org.slf4j.Logger;

/**
 * @author lizu
 * @since 2022/4/30
 */
public class SparkK8sOperatorTaskExecutor extends AbstractK8sTaskExecutor {

    private SparkGenericKubernetesResource sparkGenericKubernetesResource;

    public SparkK8sOperatorTaskExecutor(Logger logger,
        TaskExecutionContext taskRequest) {
        super(logger, taskRequest);
    }

    @Override
    public TaskResponse run(String k8sParameterStr) throws Exception {
        TaskResponse result = new TaskResponse();
        int taskInstanceId = taskRequest.getTaskInstanceId();
        K8sSparkOperatorTaskMainParameters k8STaskMainParameters = JSONUtils
            .parseObject(k8sParameterStr, K8sSparkOperatorTaskMainParameters.class);
        try {
            if (null == TaskExecutionContextCacheManager.getByTaskInstanceId(taskInstanceId)) {
                result.setExitStatusCode(EXIT_CODE_KILL);
                return result;
            }
            if (StringUtils.isEmpty(k8sParameterStr)) {
                TaskExecutionContextCacheManager.removeByTaskInstanceId(taskInstanceId);
                return result;
            }
            k8sUtils.buildNoAuthClient("https://kubernetes.docker.internal:6443");
            submitJob2k8s(k8sParameterStr);
            registerBatchJobWatcher(Integer.toString(taskInstanceId), result,
                k8STaskMainParameters);
        } catch (Exception e) {
            result.setExitStatusCode(EXIT_CODE_FAILURE);
            throw e;
        }
        return result;
    }


    @Override
    public void submitJob2k8s(String k8sParameterStr) {
        int taskInstanceId = taskRequest.getTaskInstanceId();
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        K8sSparkOperatorTaskMainParameters k8sSparkOperatorTaskMainParameters = JSONUtils
            .parseObject(k8sParameterStr, K8sSparkOperatorTaskMainParameters.class);

        try {
            logger.info("[K8sOperatorJobExecutor-{}-{}] start to submit job", taskName,
                taskInstanceId);

            sparkGenericKubernetesResource = buildK8sOperatorJob(
                k8sSparkOperatorTaskMainParameters);

            stopJobOnK8s(k8sParameterStr);

            k8sUtils.createSparkOperatorJob(k8sSparkOperatorTaskMainParameters.getNamespaceName(),
                sparkGenericKubernetesResource);
            logger.info("[K8sOperatorJobExecutor-{}-{}]  submitted job successfully", taskName,
                taskInstanceId);
        } catch (Exception e) {
            logger.error("[K8sOperatorJobExecutor-{}-{}]  fail to submit job", taskName,
                taskInstanceId);
            throw new TaskException("K8sOperatorJobExecutor fail to submit job", e);
        }


    }

    private void registerBatchJobWatcher(String taskInstanceId, TaskResponse taskResponse,
        K8sSparkOperatorTaskMainParameters k8STaskMainParameters) {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Watcher<GenericKubernetesResource> watcher = new Watcher<GenericKubernetesResource>() {
            @Override
            public void eventReceived(Action action, GenericKubernetesResource resource) {
                if (action != Action.ADDED) {
                    int jobStatus = getK8sJobStatus(resource);
                    logger.info("watch spark operator :{}", jobStatus);
                    setTaskStatus(jobStatus, taskInstanceId, taskResponse, k8STaskMainParameters);
                    if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
                        countDownLatch.countDown();
                    }
                }
            }

            @Override
            public void onClose(WatcherException e) {
                logStringBuffer.append(String.format("[K8sJobExecutor-%s] fail in k8s: %s",
                    sparkGenericKubernetesResource.getMetadata().getName(), e.getMessage()));
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
            watch = k8sUtils.createBatchSparkOperatorJobWatcher(
                sparkGenericKubernetesResource.getMetadata().getName(), watcher);
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
            logger.error("operator job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
        } catch (Exception e) {
            logger.error("operator job failed in k8s: {}", e.getMessage(), e);
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
                .get("applicationState");
            String state = applicationState.get("state").toString();
            if (state.equals(SPARK_K8S_OPERATOR_COMPLETED)) {
                return EXIT_CODE_SUCCESS;
            } else if (state.equals(SPARK_K8S_OPERATOR_FAILED)) {
                return EXIT_CODE_FAILURE;
            } else {
                return TaskConstants.RUNNING_CODE;
            }
        }
        return TaskConstants.RUNNING_CODE;
    }

    private void setTaskStatus(int jobStatus, String taskInstanceId, TaskResponse taskResponse,
        K8sTaskMainParameters k8STaskMainParameters) {
        if (jobStatus == EXIT_CODE_SUCCESS || jobStatus == EXIT_CODE_FAILURE) {
            if (null == TaskExecutionContextCacheManager
                .getByTaskInstanceId(Integer.valueOf(taskInstanceId))) {
                logStringBuffer.append(String.format("[K8sSparkOperatorJobExecutor-%s] killed",
                    sparkGenericKubernetesResource.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_KILL);
            } else if (jobStatus == EXIT_CODE_SUCCESS) {
                logStringBuffer
                    .append(String.format("[K8sSparkOperatorJobExecutor-%s] succeed in k8s",
                        sparkGenericKubernetesResource.getMetadata().getName()));
                taskResponse.setExitStatusCode(EXIT_CODE_SUCCESS);
            } else {
                String errorMessage = k8sUtils
                    .getPodLog(sparkGenericKubernetesResource.getMetadata().getName(),
                        k8STaskMainParameters.getNamespaceName());
                logStringBuffer
                    .append(String.format("[K8sSparkOperatorJobExecutor-%s] fail in k8s: %s",
                        sparkGenericKubernetesResource.getMetadata().getName(), errorMessage));
                taskResponse.setExitStatusCode(EXIT_CODE_FAILURE);
            }
        }
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        K8sSparkOperatorTaskMainParameters k8sSparkOperatorTaskMainParameters = JSONUtils
            .parseObject(k8sParameterStr, K8sSparkOperatorTaskMainParameters.class);
        String namespaceName = k8sSparkOperatorTaskMainParameters.getNamespaceName();
        String jobName = sparkGenericKubernetesResource.getMetadata().getName();
        try {
            if (Boolean.TRUE.equals(k8sUtils.sparkOperatorJobExist(jobName, namespaceName))) {
                k8sUtils.deleteSparkOperatorJob(namespaceName, sparkGenericKubernetesResource);
            }
        } catch (Exception e) {
            logger.error("[K8sJobExecutor-{}]  fail to stop job", jobName);
            throw new TaskException("K8sJobExecutor fail to stop job", e);
        }
    }

    @Override
    public void cancelApplication(String k8sParameterStr) {
        if (sparkGenericKubernetesResource != null) {
            stopJobOnK8s(k8sParameterStr);
        }
    }

    /**
     * TODO 先写死
     */
    private SparkGenericKubernetesResource buildK8sOperatorJob(
        K8sSparkOperatorTaskMainParameters k8sSparkOperatorTaskMainParameters) {

        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);

        SparkGenericKubernetesResource sparkGenericKubernetesResource = new SparkGenericKubernetesResource();
        sparkGenericKubernetesResource.setApiVersion(SPARK_K8S_OPERATOR_APIVERSION);
        sparkGenericKubernetesResource.setKind(SPARK_K8S_OPERATOR_KIND);
        ObjectMeta meta = new ObjectMeta();
        meta.setName(k8sJobName);
        meta.setNamespace(k8sSparkOperatorTaskMainParameters.getNamespaceName());
        sparkGenericKubernetesResource.setMetadata(meta);

        SparkOperatorSpec spec = new SparkOperatorSpec();
        spec.setMode(SPARK_K8S_OPERATOR_MODEL);
        spec.setType("Scala");
        spec.setImagePullPolicy("IfNotPresent");
        spec.setMainClass("org.apache.spark.examples.SparkPi");
        spec.setMainApplicationFile(
            "local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar");
        spec.setSparkVersion("3.0.0");
        spec.setImage(k8sSparkOperatorTaskMainParameters.getImage());
        RestartPolicy restartPolicy = new RestartPolicy();
        restartPolicy.setType("Never");
        spec.setRestartPolicy(restartPolicy);

        Volume volume = volume();
        spec.setVolumes(Lists.newArrayList(volume));

        Driver driver = driver();
        spec.setDriver(driver);

        Executor executor = executor();
        spec.setExecutor(executor);
        sparkGenericKubernetesResource.setSpec(spec);
        return sparkGenericKubernetesResource;
    }

    private Volume volume() {
        Volume volume = new Volume();
        volume.setName("test-volume");
        HostPath hostPath = new HostPath();
        hostPath.setPath("/tmp");
        hostPath.setType("Directory");
        volume.setHostPath(hostPath);
        return volume;
    }

    private Driver driver() {
        Driver driver = new Driver();
        Map<String, Object> envVars = new HashMap<>();
        envVars.put("lizu", "哈哈哈哈");
        driver.setEnvVars(envVars);
        driver.setCores(1);
        driver.setCoreLimit("1200m");
        driver.setMemory("512m");
        Labels labels = new Labels();
        labels.setVersion("3.0.0");
        driver.setLabels(labels);
        driver.setServiceAccount("spark");
        VolumeMounts volumeMounts = new VolumeMounts();
        volumeMounts.setName("test-volume");
        volumeMounts.setMountPath("/tmp");
        driver.setVolumeMounts(Lists.newArrayList(volumeMounts));
        return driver;
    }

    private Executor executor() {
        Executor executor = new Executor();
        executor.setCores(1);
        executor.setInstances(1);
        executor.setMemory("512m");
        Labels execLabels = new Labels();
        execLabels.setVersion("3.0.0");
        executor.setLabels(execLabels);
        VolumeMounts execVolumeMounts = new VolumeMounts();
        execVolumeMounts.setName("test-volume");
        execVolumeMounts.setMountPath("/tmp");
        executor.setVolumeMounts(Lists.newArrayList(execVolumeMounts));
        return executor;
    }

}
