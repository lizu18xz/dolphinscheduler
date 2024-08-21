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

import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.FlinkOperatorConstant;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

@Slf4j
public class K8sUtils {

    private KubernetesClient client;

    public void createJob(String namespace, Job job) {
        try {
            client.batch()
                    .v1()
                    .jobs()
                    .inNamespace(namespace)
                    .create(job);
        } catch (Exception e) {
            throw new TaskException("fail to create job", e);
        }
    }

    public void deleteJob(String jobName, String namespace) {
        try {
            client.batch()
                    .v1()
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
            JobList jobList = client.batch().jobs().inNamespace(namespace).list();
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
            return client.batch()
                    .v1()
                    .jobs()
                    .withName(jobName)
                    .watch(watcher);
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
                if (podName.contains("-") && jobName
                        .equals(podName.substring(0, podName.lastIndexOf("-")))) {
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
            log.info("client config:{}",configYaml);
            client = new KubernetesClientBuilder().withConfig(config).build();
        } catch (Exception e) {
            throw new TaskException("fail to build k8s ApiClient", e);
        }
    }

    public void loadFlinkOperatorJob(String yaml) {
        try {
            client.load(new ByteArrayInputStream((yaml).getBytes())).createOrReplace();
        } catch (Exception e) {
            throw new TaskException("fail to create flink job", e);
        }
    }

    public void deleteFlinkOperatorJob(String yaml) {
        try {
            client
                    .load(new ByteArrayInputStream((yaml).getBytes()))
                    .delete();
        } catch (Exception e) {
            throw new TaskException("fail to delete flink job", e);
        }
    }

    public Boolean flinkOperatorJobExist(String jobName, String namespace) {
        Optional<GenericKubernetesResource> result;
        try {
            log.info("check job exist jobName:{},namespace:{} ", jobName, namespace);
            CustomResourceDefinitionContext context = getFlinkCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                    client.genericKubernetesResources(context);
            GenericKubernetesResourceList list = resourceMixedOperation
                    .inNamespace(namespace).list();
            List<GenericKubernetesResource> items = list.getItems();
            result = items.stream()
                    .filter(job -> job.getMetadata().getName().equals(jobName))
                    .findFirst();
            return result.isPresent();
        } catch (Exception e) {
            e.printStackTrace();
            throw new TaskException("fail to check flink job: ", e);
        }
    }

    public Watch createBatchFlinkOperatorJobWatcher(String jobName,
                                                    Watcher<GenericKubernetesResource> watcher) {
        try {
            CustomResourceDefinitionContext context = getFlinkCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                    client.genericKubernetesResources(context);
            return resourceMixedOperation.withName(jobName).watch(watcher);
        } catch (Exception e) {
            throw new TaskException("fail to register flink operator batch job watcher", e);
        }
    }

    private CustomResourceDefinitionContext getFlinkCustomResourceDefinitionContext() {
        return new CustomResourceDefinitionContext.Builder()
                .withGroup(FlinkOperatorConstant.API_GROUP)
                .withVersion(FlinkOperatorConstant.API_VERSION)
                .withScope(FlinkOperatorConstant.NAMESPACED)
                .withPlural(FlinkOperatorConstant.FLINK_DEPLOYMENTS)
                .withKind(FlinkOperatorConstant.KIND_FLINK_DEPLOYMENT)
                .build();
    }


    public Boolean pytorchJobExist(String jobName, String namespace) {
        Optional<GenericKubernetesResource> result;
        try {
            log.info("check pytorch job exist jobName:{},namespace:{} ", jobName, namespace);
            CustomResourceDefinitionContext context = getPytorchCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                    client.genericKubernetesResources(context);
            GenericKubernetesResourceList list = resourceMixedOperation
                    .inNamespace(namespace).list();
            List<GenericKubernetesResource> items = list.getItems();
            result = items.stream()
                    .filter(job -> job.getMetadata().getName().equals(jobName))
                    .findFirst();
            return result.isPresent();
        } catch (Exception e) {
            e.printStackTrace();
            throw new TaskException("fail to check pytorch job: ", e);
        }
    }

    public Watch createPytorchJobWatcher(String jobName, String nameSpace,
                                         Watcher<GenericKubernetesResource> watcher) {
        try {
            CustomResourceDefinitionContext context = getPytorchCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                    client.genericKubernetesResources(context);
            return resourceMixedOperation.inNamespace(nameSpace).withName(jobName).watch(watcher);
        } catch (Exception e) {
            throw new TaskException("fail to register flink operator batch job watcher", e);
        }
    }

    private CustomResourceDefinitionContext getPytorchCustomResourceDefinitionContext() {
        return new CustomResourceDefinitionContext.Builder()
                .withGroup("kubeflow.org")
                .withVersion("v1")
                .withScope("Namespaced")
                .withPlural("pytorchjobs")
                .withKind("PyTorchJob")
                .build();
    }

    public void loadApplyYmlJob(String yaml) {
        try {
            client.load(new ByteArrayInputStream((yaml).getBytes())).createOrReplace();
        } catch (Exception e) {
            throw new TaskException("fail to create flink job", e);
        }
    }

    public void deleteApplyYmlJob(String yaml) {
        try {
            client
                    .load(new ByteArrayInputStream((yaml).getBytes()))
                    .delete();
        } catch (Exception e) {
            throw new TaskException("fail to delete apply job", e);
        }
    }

}
