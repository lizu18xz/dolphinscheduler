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

package org.apache.dolphinscheduler.plugin.task.api.k8s;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.CLUSTER;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.EXIT_CODE_KILL;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.NAMESPACE_NAME;
import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.UNIQUE_LABEL_NAME;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.batch.v1.JobList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.utils.Serialization;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.FlinkDeployment;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.FlinkOperatorConstant;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.status.FlinkDeploymentStatus;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.utils.JobStatusEnums;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.FlinkK8sOperatorTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.K8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.dolphinscheduler.plugin.task.api.utils.K8sUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;

public class K8sTaskExecutorTest {

    private static final Logger logger = LoggerFactory.getLogger(K8sTaskExecutorTest.class);

    private K8sTaskExecutor k8sTaskExecutor = null;
    private K8sTaskMainParameters k8sTaskMainParameters = null;
    private final String image = "ds-dev";
    private final String imagePullPolicy = "IfNotPresent";
    private final String namespace = "{\"name\":\"default\",\"cluster\":\"lab\"}";
    private final double minCpuCores = 2;
    private final double minMemorySpace = 10;
    private final int taskInstanceId = 1000;
    private final String taskName = "k8s_task_test";
    private Job job;

    @BeforeEach
    public void before() {
        TaskExecutionContext taskRequest = new TaskExecutionContext();
        taskRequest.setTaskInstanceId(taskInstanceId);
        taskRequest.setTaskName(taskName);
        Map<String, String> namespace = JSONUtils.toMap(this.namespace);
        String namespaceName = namespace.get(NAMESPACE_NAME);
        String clusterName = namespace.get(CLUSTER);
        Map<String, String> labelMap = new HashMap<>();
        labelMap.put("test", "1234");

        NodeSelectorRequirement requirement = new NodeSelectorRequirement();
        requirement.setKey("node-label");
        requirement.setOperator("In");
        requirement.setValues(Arrays.asList("1234", "123456"));
        k8sTaskExecutor = new K8sTaskExecutor(logger, taskRequest);
        k8sTaskMainParameters = new K8sTaskMainParameters();
        k8sTaskMainParameters.setImage(image);
        k8sTaskMainParameters.setImagePullPolicy(imagePullPolicy);
        k8sTaskMainParameters.setNamespaceName(namespaceName);
        k8sTaskMainParameters.setClusterName(clusterName);
        k8sTaskMainParameters.setMinCpuCores(minCpuCores);
        k8sTaskMainParameters.setMinMemorySpace(minMemorySpace);
        k8sTaskMainParameters
            .setCommand("[\"perl\" ,\"-Mbignum=bpi\", \"-wle\", \"print bpi(2000)\"]");
        k8sTaskMainParameters.setLabelMap(labelMap);
        k8sTaskMainParameters.setNodeSelectorRequirements(Arrays.asList(requirement));
        job = k8sTaskExecutor.buildK8sJob(k8sTaskMainParameters);
    }

    @Test
    public void testGetK8sJobStatusNormal() {
        JobStatus jobStatus = new JobStatus();
        jobStatus.setSucceeded(1);
        job.setStatus(jobStatus);
        Assertions.assertEquals(0, Integer.compare(0, k8sTaskExecutor.getK8sJobStatus(job)));
    }

    @Test
    public void testSetTaskStatusNormal() {
        int jobStatus = 0;
        TaskResponse taskResponse = new TaskResponse();
        k8sTaskExecutor.setJob(job);
        k8sTaskExecutor.setTaskStatus(jobStatus, String.valueOf(taskInstanceId), taskResponse);
        Assertions
            .assertEquals(0, Integer.compare(EXIT_CODE_KILL, taskResponse.getExitStatusCode()));
    }

    @Test
    public void testWaitTimeoutNormal() {
        try {
            k8sTaskExecutor.waitTimeout(true);
        } catch (TaskException e) {
            Assertions.assertEquals(e.getMessage(), "K8sTask is timeout");
        }
    }

    public static void main(String[] args) {
        try {
            Config config = Config.fromKubeconfig("apiVersion: v1\n"
                + "clusters:\n"
                + "- cluster:\n"
                + "    server: https://kubernetes.docker.internal:6443\n"
                + "kind: Config");

            KubernetesClient client = new KubernetesClientBuilder().withConfig(config).build();
            System.out.println("AAAAAAA");

            // jobName:seatun01-2,namespace:flinktest
            if (Boolean.TRUE.equals(flinkOperatorJobExist(client, "seatun01-2", "flinktest"))) {
                System.out.println("aaaaa exist");

                deleteFlinkOperatorJob(client,"",null);
            } else {
                System.out.println("aaaaaaaa exist");
            }
            // ccc(client);
            System.out.println("bbbbbb");
        } catch (Exception e) {
            e.printStackTrace();
            throw new TaskException("fail to build k8s ApiClient", e);
        }
    }

    public static void deleteFlinkOperatorJob(KubernetesClient client, String namespace,
        FlinkDeployment flinkDeployment) {
        String yml="---\n"
            + "apiVersion: \"flink.apache.org/v1beta1\"\n"
            + "kind: \"FlinkDeployment\"\n"
            + "metadata:\n"
            + "  name: \"seatun01-2\"\n"
            + "  namespace: \"flinktest\"\n"
            + "spec:\n"
            + "  job:\n"
            + "    jarURI: \"local:///opt/seatunnel/starter/seatunnel-flink-15-starter.jar\"\n"
            + "    parallelism: 1\n"
            + "    entryClass: \"org.apache.seatunnel.core.starter.flink.SeaTunnelFlink\"\n"
            + "    args:\n"
            + "    - \"--config\"\n"
            + "    - \"/data/seatunnel.conf\"\n"
            + "    state: \"running\"\n"
            + "    upgradeMode: \"stateless\"\n"
            + "  flinkConfiguration:\n"
            + "    taskmanager.numberOfTaskSlots: \"1\"\n"
            + "  image: \"registry.cn-hangzhou.aliyuncs.com/lz18xz/lizu:seatunnel-2.3.3-flink-1.15-0830\"\n"
            + "  imagePullPolicy: \"IfNotPresent\"\n"
            + "  serviceAccount: \"default\"\n"
            + "  flinkVersion: \"v1_15\"\n"
            + "  podTemplate:\n"
            + "    apiVersion: \"v1\"\n"
            + "    kind: \"Pod\"\n"
            + "    metadata:\n"
            + "      labels:\n"
            + "        dolphinscheduler-label: \"2_2\"\n"
            + "    spec:\n"
            + "      containers:\n"
            + "      - env:\n"
            + "        - name: \"system.task.definition.name\"\n"
            + "          value: \"seatun01\"\n"
            + "        - name: \"system.project.name\"\n"
            + "        - name: \"system.project.code\"\n"
            + "          value: \"10800342458912\"\n"
            + "        - name: \"system.workflow.instance.id\"\n"
            + "          value: \"2\"\n"
            + "        - name: \"system.biz.curdate\"\n"
            + "          value: \"20230904\"\n"
            + "        - name: \"system.biz.date\"\n"
            + "          value: \"20230903\"\n"
            + "        - name: \"system.task.instance.id\"\n"
            + "          value: \"2\"\n"
            + "        - name: \"system.workflow.definition.name\"\n"
            + "          value: \"seatun01\"\n"
            + "        - name: \"system.task.definition.code\"\n"
            + "          value: \"10800343396512\"\n"
            + "        - name: \"system.workflow.definition.code\"\n"
            + "          value: \"10800345150880\"\n"
            + "        - name: \"system.datetime\"\n"
            + "          value: \"20230904143611\"\n"
            + "        name: \"flink-main-container\"\n"
            + "        volumeMounts:\n"
            + "        - mountPath: \"/data/seatunnel.conf\"\n"
            + "          name: \"seatunnel-conf-volume\"\n"
            + "          subPath: \"seatunnel.conf\"\n"
            + "      volumes:\n"
            + "      - configMap:\n"
            + "          items:\n"
            + "          - key: \"seatunnel.conf\"\n"
            + "            path: \"seatunnel.conf\"\n"
            + "          name: \"seatun01-2-seatunnel-configmap\"\n"
            + "        name: \"seatunnel-conf-volume\"\n"
            + "  jobManager:\n"
            + "    resource:\n"
            + "      cpu: 0.5\n"
            + "      memory: \"1024m\"\n"
            + "    replicas: 1\n"
            + "  taskManager:\n"
            + "    resource:\n"
            + "      cpu: 0.5\n"
            + "      memory: \"1024m\"\n"
            + "status:\n"
            + "  jobStatus:\n"
            + "    savepointInfo:\n"
            + "      savepointHistory: []\n"
            + "      lastPeriodicSavepointTimestamp: 0\n"
            + "  lifecycleState: \"CREATED\"\n"
            + "  clusterInfo: {}\n"
            + "  jobManagerDeploymentStatus: \"MISSING\"\n"
            + "  reconciliationStatus:\n"
            + "    reconciliationTimestamp: 0\n"
            + "    state: \"UPGRADING\"\n"
            + "---\n"
            + "apiVersion: \"v1\"\n"
            + "kind: \"ConfigMap\"\n"
            + "metadata:\n"
            + "  name: \"seatun01-2-seatunnel-configmap\"\n"
            + "  namespace: \"flinktest\"\n"
            + "data:\n"
            + "  seatunnel.conf: \"env {\\n  execution.parallelism = 2\\n  job.mode = \\\"STREAMING\\\"\\n\\\n"
            + "    \\  checkpoint.interval = 10000\\n}\\n\\nsource {\\n  FakeSource {\\n    parallelism\\\n"
            + "    \\ = 2\\n    result_table_name = \\\"fake\\\"\\n    row.num = 16\\n    schema = {\\n  \\\n"
            + "    \\    fields {\\n        name = \\\"string\\\"\\n        age = \\\"int\\\"\\n      }\\n   \\\n"
            + "    \\ }\\n  }\\n}\\n\\nsink {\\n  Console {\\n  }\\n}\"\n";

        try {
            client
                .load(new ByteArrayInputStream((yml).getBytes()))
                .delete();
        } catch (Exception e) {
            e.printStackTrace();
            throw new TaskException("fail to delete flink job", e);
        }
    }


    private static CustomResourceDefinitionContext getFlinkCustomResourceDefinitionContext() {
        return new CustomResourceDefinitionContext.Builder()
            .withGroup(FlinkOperatorConstant.API_GROUP)
            .withVersion(FlinkOperatorConstant.API_VERSION)
            .withScope(FlinkOperatorConstant.NAMESPACED)
            .withPlural(FlinkOperatorConstant.FLINK_DEPLOYMENTS)
            .withKind(FlinkOperatorConstant.KIND_FLINK_DEPLOYMENT)
            .build();
    }

    public static Boolean flinkOperatorJobExist(KubernetesClient client, String jobName,
        String namespace) {
        Optional<GenericKubernetesResource> result;
        try {
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

    public static void ccc(KubernetesClient client) {
        String yaml = "---\n"
            + "apiVersion: \"flink.apache.org/v1beta1\"\n"
            + "kind: \"FlinkDeployment\"\n"
            + "metadata:\n"
            + "  name: \"lzseatu-6\"\n"
            + "  namespace: \"default\"\n"
            + "spec:\n"
            + "  job:\n"
            + "    jarURI: \"local:///opt/seatunnel/starter/seatunnel-flink-15-starter.jar\"\n"
            + "    parallelism: 1\n"
            + "    entryClass: \"org.apache.seatunnel.core.starter.flink.SeaTunnelFlink\"\n"
            + "    args:\n"
            + "    - \"--config\"\n"
            + "    - \"/data/seatunnel.conf\"\n"
            + "    state: \"running\"\n"
            + "    upgradeMode: \"stateless\"\n"
            + "  flinkConfiguration:\n"
            + "    taskmanager.numberOfTaskSlots: \"1\"\n"
            + "  image: \"registry.cn-hangzhou.aliyuncs.com/lz18xz/lizu:seatunnel-2.3.3-flink-1.15-0830\"\n"
            + "  imagePullPolicy: \"IfNotPresent\"\n"
            + "  serviceAccount: \"flink\"\n"
            + "  flinkVersion: \"v1_15\"\n"
            + "  podTemplate:\n"
            + "    apiVersion: \"v1\"\n"
            + "    kind: \"Pod\"\n"
            + "    metadata:\n"
            + "      labels:\n"
            + "        dolphinscheduler-label: \"6_6\"\n"
            + "    spec:\n"
            + "      containers:\n"
            + "      - env:\n"
            + "        - name: \"system.task.definition.name\"\n"
            + "          value: \"lzseatu\"\n"
            + "        - name: \"system.project.name\"\n"
            + "        - name: \"system.project.code\"\n"
            + "          value: \"10793891911968\"\n"
            + "        - name: \"system.workflow.instance.id\"\n"
            + "          value: \"6\"\n"
            + "        - name: \"system.biz.curdate\"\n"
            + "          value: \"20230904\"\n"
            + "        - name: \"system.biz.date\"\n"
            + "          value: \"20230903\"\n"
            + "        - name: \"system.task.instance.id\"\n"
            + "          value: \"6\"\n"
            + "        - name: \"system.workflow.definition.name\"\n"
            + "          value: \"lzse\"\n"
            + "        - name: \"system.task.definition.code\"\n"
            + "          value: \"10793893035680\"\n"
            + "        - name: \"system.workflow.definition.code\"\n"
            + "          value: \"10793895298080\"\n"
            + "        - name: \"system.datetime\"\n"
            + "          value: \"20230904003109\"\n"
            + "        name: \"flink-main-container\"\n"
            + "        volumeMounts:\n"
            + "        - mountPath: \"/data/seatunnel.conf\"\n"
            + "          name: \"seatunnel-conf-volume\"\n"
            + "          subPath: \"seatunnel.conf\"\n"
            + "      volumes:\n"
            + "      - configMap:\n"
            + "          items:\n"
            + "          - key: \"seatunnel.conf\"\n"
            + "            path: \"seatunnel.conf\"\n"
            + "          name: \"lzseatu-6-seatunnel-configmap\"\n"
            + "        name: \"seatunnel-conf-volume\"\n"
            + "  jobManager:\n"
            + "    resource:\n"
            + "      cpu: 0.5\n"
            + "      memory: \"1024m\"\n"
            + "    replicas: 1\n"
            + "  taskManager:\n"
            + "    resource:\n"
            + "      cpu: 0.5\n"
            + "      memory: \"1024m\"\n"
            + "---\n"
            + "apiVersion: \"v1\"\n"
            + "kind: \"ConfigMap\"\n"
            + "metadata:\n"
            + "  name: \"lzseatu-6-seatunnel-configmap\"\n"
            + "  namespace: \"default\"\n"
            + "data:\n"
            + "  seatunnel.conf: \"env {\\n  execution.parallelism = 2\\n  job.mode = \\\"BATCH\\\"\\n  checkpoint.interval\\\n"
            + "    \\ = 10000\\n}\\n\\nsource {\\n  FakeSource {\\n    parallelism = 1\\n    result_table_name\\\n"
            + "    \\ = \\\"fake\\\"\\n    row.num = 16\\n    schema = {\\n      fields {\\n        name =\\\n"
            + "    \\ \\\"string\\\"\\n        age = \\\"int\\\"\\n      }\\n    }\\n  }\\n}\\n\\nsink {\\n  Console\\\n"
            + "    \\ {\\n  }\\n}\"";

        client.load(new ByteArrayInputStream((yaml).getBytes())).createOrReplace();

        CustomResourceDefinitionContext context = new CustomResourceDefinitionContext.Builder()
            .withGroup("flink.apache.org")
            .withVersion("v1beta1")
            .withScope("Namespaced")
            .withName("flink-kubernetes-operator")
            .withPlural("flinkdeployments")
            .withKind("FlinkDeployment")
            .build();
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
            client.genericKubernetesResources(context);

        //监控任务的运行

        resourceMixedOperation.withName("lzseatu-6")
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

                            FlinkDeploymentStatus flinkDeploymentStatus = JSONUtils
                                .parseObject(JSONUtils.toJsonString(status),
                                    FlinkDeploymentStatus.class);
                            Map<String, Object> state = (Map<String, Object>) status
                                .get("jobStatus");
                            if (state != null) {
                                String state1 = state.get("state").toString();
                                System.out.println(state1.toString());
                            }
                        }
                    }
                }

                @Override
                public void onClose(WatcherException cause) {
                    System.out.println("close~~~");
                }
            });

        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourcew =
            client.genericKubernetesResources("v1", "Pod");

        Map<String, String> labels = new HashMap<>();
        //component=jobmanager,dolphinscheduler-label=6_6
        labels.put("dolphinscheduler-label", "6_6");
        labels.put("component", "jobmanager");
        resourcew.withLabels(labels).watch(new Watcher<GenericKubernetesResource>() {
            @Override
            public void eventReceived(Action action, GenericKubernetesResource resource) {
                System.out.println("eventReceived222222~~~");
                if (action != Action.ADDED) {

                    Map<String, Object> additionalProperties = resource
                        .getAdditionalProperties();
                    if (additionalProperties != null) {
                        Map<String, Object> status = (Map<String, Object>) additionalProperties
                            .get("status");
                        FlinkDeploymentStatus flinkDeploymentStatus = JSONUtils
                            .parseObject(JSONUtils.toJsonString(status),
                                FlinkDeploymentStatus.class);
                        Map<String, Object> state = (Map<String, Object>) status
                            .get("jobStatus");
                        if (state != null) {
                            String state1 = state.get("state").toString();
                            System.out.println(state1.toString());
                        }
                    }
                }
            }

            @Override
            public void onClose(WatcherException cause) {
                System.out.println("close~~~");
            }
        });


    }

}
