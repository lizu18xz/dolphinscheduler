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

package org.apache.dolphinscheduler.plugin.task.api.utils;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.LOG_LINES;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.*;

import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver.Labels;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver.VolumeMounts;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Executor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.RestartPolicy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.SparkGenericKubernetesResource;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.SparkOperatorSpec;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Volume;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Volume.HostPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class K8sUtils {

    private static final Logger log = LoggerFactory.getLogger(K8sUtils.class);
    private KubernetesClient client;

    public void createJob(String namespace, Job job) {
        try {
            client.batch().v1()
                .jobs()
                .inNamespace(namespace)
                .create(job);
        } catch (Exception e) {
            throw new TaskException("fail to create job", e);
        }
    }

    public void deleteJob(String jobName, String namespace) {
        try {
            client.batch().v1()
                .jobs()
                .inNamespace(namespace)
                .withName(jobName)
                .delete();
        } catch (Exception e) {
            throw new TaskException("fail to delete job", e);
        }
    }

    public Boolean jobExist(String jobName, String namespace) {
        Optional<Job> result;
        try {
            JobList jobList = client.batch().v1().jobs().inNamespace(namespace).list();
            List<Job> jobs = jobList.getItems();
            result = jobs.stream()
                .filter(job -> job.getMetadata().getName().equals(jobName))
                .findFirst();
            return result.isPresent();
        } catch (Exception e) {
            throw new TaskException("fail to check job: ", e);
        }
    }

    public Watch createBatchJobWatcher(String jobName, Watcher<Job> watcher) {
        try {
            return client.batch().v1()
                .jobs().withName(jobName).watch(watcher);
        } catch (Exception e) {
            throw new TaskException("fail to register batch job watcher", e);
        }
    }

    public String getPodLog(String jobName, String namespace) {
        try {
            List<Pod> podList = client.pods().inNamespace(namespace).list().getItems();
            String podName = null;
            for (Pod pod : podList) {
                podName = pod.getMetadata().getName();
                if (jobName
                    .equals(podName.substring(0, pod.getMetadata().getName().lastIndexOf("-")))) {
                    break;
                }
            }
            return client.pods().inNamespace(namespace)
                .withName(podName)
                .tailingLines(LOG_LINES)
                .getLog(Boolean.TRUE);
        } catch (Exception e) {
            log.error("fail to getPodLog", e);
            log.error("response bodies : {}", e.getMessage());
        }
        return null;
    }

    public void buildClient(String configYaml) {
        try {
            Config config = Config.fromKubeconfig(configYaml);
            client = new DefaultKubernetesClient(config);
        } catch (Exception e) {
            throw new TaskException("fail to build k8s ApiClient", e);
        }
    }

    public void buildNoAuthClient(String masterUrl) {
        try {
            Config config = new ConfigBuilder()
                .withMasterUrl(masterUrl)
                .build();
            client = new DefaultKubernetesClient(config);
        } catch (Exception e) {
            throw new TaskException("fail to build k8s ApiClient", e);
        }
    }

    /**
     * TODO 参数优化
     */
    public void createSparkOperatorJob(String namespace,
        SparkGenericKubernetesResource sparkGenericKubernetesResource) {
        try {
            CustomResourceDefinitionContext context = getSparkCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                client.genericKubernetesResources(context);

            resourceMixedOperation.inNamespace("spark-operator")
                .createOrReplace(sparkGenericKubernetesResource);

        } catch (Exception e) {
            throw new TaskException("fail to create job", e);
        }
    }

    public void deleteSparkOperatorJob(String namespace,
        SparkGenericKubernetesResource sparkGenericKubernetesResource) {
        try {
            CustomResourceDefinitionContext context = getSparkCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                client.genericKubernetesResources(context);
            resourceMixedOperation.inNamespace("spark-operator")
                .delete(sparkGenericKubernetesResource);
        } catch (Exception e) {
            throw new TaskException("fail to delete job", e);
        }
    }

    public Boolean sparkOperatorJobExist(String jobName, String namespace) {
        Optional<GenericKubernetesResource> result;
        try {
            CustomResourceDefinitionContext context = getSparkCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                client.genericKubernetesResources(context);

            GenericKubernetesResourceList list = resourceMixedOperation
                .inNamespace("spark-operator").list();
            List<GenericKubernetesResource> items = list.getItems();
            result = items.stream()
                .filter(job -> job.getMetadata().getName().equals(jobName))
                .findFirst();
            return result.isPresent();
        } catch (Exception e) {
            throw new TaskException("fail to check job: ", e);
        }
    }

    public Watch createBatchSparkOperatorJobWatcher(String jobName,
        Watcher<GenericKubernetesResource> watcher) {
        try {
            CustomResourceDefinitionContext context = getSparkCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                client.genericKubernetesResources(context);
            return resourceMixedOperation.withName(jobName).watch(watcher);
        } catch (Exception e) {
            throw new TaskException("fail to register spark operator batch job watcher", e);
        }
    }

    private CustomResourceDefinitionContext getSparkCustomResourceDefinitionContext() {
        return new CustomResourceDefinitionContext.Builder()
            .withGroup(SPARK_K8S_OPERATOR_GROUP)
            .withVersion(SPARK_K8S_OPERATOR_VERSION)
            .withScope(SPARK_K8S_OPERATOR_SCOPE)
            .withName(SPARK_K8S_OPERATOR_NAME)
            .withPlural(SPARK_K8S_OPERATOR_PLURAL)
            .withKind(SPARK_K8S_OPERATOR_KIND)
            .build();
    }


    public static void main(String[] args) {
        try {
            Config config = new ConfigBuilder()
                .withMasterUrl("https://kubernetes.docker.internal:6443")
                .build();
            KubernetesClient client = new DefaultKubernetesClient(config);
            CustomResourceDefinitionContext context = new CustomResourceDefinitionContext.Builder()
                .withGroup("sparkoperator.k8s.io")
                .withVersion("v1beta2")
                .withScope("Namespaced")
                .withName("spark-operator-task")
                .withPlural("sparkapplications")
                .withKind("SparkApplication")
                .build();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                client.genericKubernetesResources(context);

            //或者集成  GenericKubernetesResource

            SparkGenericKubernetesResource sparkGenericKubernetesResource = getSparkGenericKubernetesResource();

            //文件流
            /*resourceMixedOperation.inNamespace("spark-operator")
                .load(K8sUtils.class.getResourceAsStream("/spark-pi.yaml"))
                .createOrReplace();*/

            resourceMixedOperation.inNamespace("spark-operator")
                .delete(sparkGenericKubernetesResource);

            resourceMixedOperation.inNamespace("spark-operator")
                .createOrReplace(sparkGenericKubernetesResource);

            //监控任务的运行
            resourceMixedOperation.withName("spark-pi")
                .watch(new Watcher<GenericKubernetesResource>() {
                    @Override
                    public void eventReceived(Action action, GenericKubernetesResource resource) {
                        System.out.println("eventReceived~~~");
                        if (action != Action.ADDED) {
                            Map<String, Object> additionalProperties = resource
                                .getAdditionalProperties();
                            if (additionalProperties != null) {
                                Map<String, Object> status = (Map<String, Object>) additionalProperties
                                    .get("status");
                                Map<String, Object> state = (Map<String, Object>) status
                                    .get("applicationState");
                                String state1 = state.get("state").toString();
                                System.out.println(state1.toString());
                            }
                        }
                    }

                    @Override
                    public void onClose(WatcherException cause) {
                        System.out.println("close~~~");
                    }
                });


            System.out.println("哈哈");

        } catch (Exception e) {
            log.error("fail to build k8s ApiClient", e);
            throw new TaskException("fail to build k8s ApiClient");
        }
    }

    private static SparkGenericKubernetesResource getSparkGenericKubernetesResource() {
        SparkGenericKubernetesResource sparkResource = new SparkGenericKubernetesResource();
        sparkResource.setApiVersion("sparkoperator.k8s.io/v1beta2");
        sparkResource.setKind("SparkApplication");
        ObjectMeta meta = new ObjectMeta();
        meta.setName("spark-pi");
        meta.setNamespace("spark-operator");
        sparkResource.setMetadata(meta);

        SparkOperatorSpec spec = new SparkOperatorSpec();
        spec.setMode("cluster");
        spec.setType("Scala");
        spec.setImagePullPolicy("IfNotPresent");
        spec.setMainClass("org.apache.spark.examples.SparkPi");
        spec.setMainApplicationFile(
            "local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar");
        spec.setSparkVersion("3.0.0");
        spec.setImage("registry.cn-hangzhou.aliyuncs.com/terminus/spark:v3.0.0");
        RestartPolicy restartPolicy = new RestartPolicy();
        restartPolicy.setType("Never");
        spec.setRestartPolicy(restartPolicy);

        Volume volume = new Volume();
        volume.setName("test-volume");
        HostPath hostPath = new HostPath();
        hostPath.setPath("/tmp");
        hostPath.setType("Directory");
        volume.setHostPath(hostPath);
        spec.setVolumes(Lists.newArrayList(volume));

        Driver driver = new Driver();
        Map<String, Object> envVars = new HashMap<>();
        envVars.put("envStr", "哈哈哈哈");
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
        spec.setDriver(driver);

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
        spec.setExecutor(executor);
        sparkResource.setSpec(spec);

        return sparkResource;
    }

}
