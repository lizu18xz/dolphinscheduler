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

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.FlinkDeployment;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.FlinkOperatorConstant;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.status.FlinkDeploymentStatus;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.K8sTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobStatus;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;

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
            Config config = Config.fromKubeconfig("apiVersion: v1\n" +
                    "clusters:\n" +
                    "- cluster:\n" +
                    "    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJVW9xaXJ2ZjcycVl3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TXpFeE1qRXlNRE13TURsYUZ3MHpNekV4TVRneU1ETTFNRGxhTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUURJbjROM2xBd20xcDQrbkJyM0Fqdm8wVzJWUFd4NUlzY3kzL0l5eXZZa2o1QVgyNG1CQ2lYUFBTbnEKMlRXaTE2VWs1UmZpejBaOFkwTnB3bWFaVjZ3c0pYcHprOEtWS3RmNnJxVm5vRVFqay9VVEFiMGV4NkFrSG9MbAovWFNVYzU5S055TkM1SzBsQ1hFdVBEWHVZUEhmUllJQm9sOTE4RUJnQVhjV0k5VlhOS1NLelNYbGd2L0FpS3dZClE3S1BQdldoZ01PZVhRU2p4clcxc1M5NldsMGh2WmdxNnVEUkhRWWR6OXBaZzJzaVZneTZwN3psV0Y1SW5KVEoKWEdFdVlxbDNFY01IMnBCSWt4WUNEckZTMWR0Vm9KQW9KUXl1VlZySGZZM0tRQWp4a0N4bUxUQUE0MXlielY2RwpnV0kwa0xlbDNNYlMrNFBTbXFvZnMyS0N4RUF6QWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJRS3NzVFZSQVRqak5wTlQ0R0x3YWJRbXpNZzNqQVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQ0I4WmpPNDRLVwp4WFczTjIyclZtQmhjazVmY0hBYUxwMnMyVXhVeTliKzNFVC80alJ3NzdYMXUvZ1R2a1haNlJXSkFMYUNCYVF4CllDU2x4K2w5aGY2akZlVXcxd1pKM094dHNIRXJPa3MzV0FWVUdTQ1U5RE9Ga3VmMkQ3a2lxUUpXM2N1bU9LaVYKMTNUeGUwNHN2azE3clFFL0J5M01JZ0tydUdRdUFIb29qWENka3dpNnhTdVh6QmorUXBiSmdrREFlTXJMNmpmOQprc2kyUElGbFBDZHpqQWp4QVMxbkV0VytXRG5RZGlNODA1SGJHRFdRb3lpOUNKdzhvSmRqeFFyOUtwdE41cEJTCmVUYnR3SWFScWE4eE03NkNUWFVDUUNIZ1JDVE4vYXE1UXFaeGlXWjN2OTRlQi9WL1liSXgvVWgwTDVJOGtGTEwKN3NkbVZHc0szZjVkCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K\n"
                    +
                    "    server: https://192.168.152.130:6443\n" +
                    "  name: kubernetes  \n" +
                    "kind: Config\n" +
                    "contexts:\n" +
                    "- context:\n" +
                    "    cluster: kubernetes\n" +
                    "    namespace: dolphinscheduler\n" +
                    "    user: dolphinscheduler\n" +
                    "  name: dolphinscheduler-context\n" +
                    "current-context: dolphinscheduler-context\n" +
                    "users:\n" +
                    "- name: dolphinscheduler\n" +
                    "  user:\n" +
                    "    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN4ekNDQWE4Q0NRREVseWJOejlhZS96QU5CZ2txaGtpRzl3MEJBUXNGQURBVk1STXdFUVlEVlFRREV3cHIKZFdKbGNtNWxkR1Z6TUI0WERUSXpNVEV5TXpFek5EYzBPRm9YRFRJMU1EUXdOakV6TkRjME9Gb3dOakVaTUJjRwpBMVVFQXd3UVpHOXNjR2hwYm5OamFHVmtkV3hsY2pFWk1CY0dBMVVFQ2d3UVpHOXNjR2hwYm5OamFHVmtkV3hsCmNqQ0NBU0l3RFFZSktvWklodmNOQVFFQkJRQURnZ0VQQURDQ0FRb0NnZ0VCQU1WQ01yR05NeGJnaFVpamVGRzYKTVRFSDlQcCtVeGgxSFJNTUVhVHVhbVhsNHRySSszYjE2N05zK05EODJZaFU3NzRuL1ZDWVloWFJYdlRCcE54RgpRUzllTGZRNDhRR0RvU0Q3S1dVU2NZVnRiRzNZU2J1aXhJNUpmZm90bk9vbDBIU0pXM1Y5L1BFZzc4SHFyU29WCmhMVnNYd3FUUG5OYnM2b1NvVktDRUsrT1ZiSSswb1pHbldBUEdkV2dVS2FKNkRuK2J6UVJrQUdBb2JzTXV6WTIKaEJkNFoxK1RGQ0lsK3hYejJ2c0N2ZlJtVGlxSVhBNWxLbkdtUnFhazhBa2pzcVZYU0dYVlFHWnVZNUhuN09VUgpCN2IzcSt5YUxpOUtSaWMxOVdxWG4yd0x6V1B5ODhxekJuYXdlRk8zelR3QkMzWjVIUFdVc0Y2UEh4emxlZjhCCnY2Y0NBd0VBQVRBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQVFFQWxiSk0xTDdZZzYzK3dWeGVqRVNudFQyUTMyWkoKOGF4Mjd0MGFkRzNBUmd4OWJLVktZKzVXdFBaUHFXSC9FWG9uMkhqY2FpdmY5NUhBekVXRDEzdnhGWjNiTW4rbwpJeGw4NkdkUy92dFBaSGpBYUNrVGxVcEtKVXZ6THBidDl6cU00Qmc2YTBQV0F3bm1xNm91eTVzeGpZM3RuYzcvCjFhV2orbEx3RjhySC9HUVhFNVVQRXc4VFdZMWV0VE1ZNkhyczJMbzljYllHZkpzVXZpQ3dIb003Zyt6ZXYzOXgKd0RPeWNtRGxpN1d1K3FwZVlOazFJcUd0Uzk0QlZ0bFYrOWdvbXhnN0R6V0pqcDIxL1UxQk1VK0ZrK3NMY3RUcApFM2JCdXRjMDJCNXdqdlFES1ZWQzlObXZBUWhmeDc1czdFdEVmRnU3T1dUaCsrc0RJaXRFaVdCQmtRPT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo\n"
                    +
                    "    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBeFVJeXNZMHpGdUNGU0tONFVib3hNUWYwK241VEdIVWRFd3dScE81cVplWGkyc2o3CmR2WHJzMno0MFB6WmlGVHZ2aWY5VUpoaUZkRmU5TUdrM0VWQkwxNHQ5RGp4QVlPaElQc3BaUkp4aFcxc2JkaEoKdTZMRWprbDkraTJjNmlYUWRJbGJkWDM4OFNEdndlcXRLaFdFdFd4ZkNwTStjMXV6cWhLaFVvSVFyNDVWc2o3Uwpoa2FkWUE4WjFhQlFwb25vT2Y1dk5CR1FBWUNodXd5N05qYUVGM2huWDVNVUlpWDdGZlBhK3dLOTlHWk9Lb2hjCkRtVXFjYVpHcHFUd0NTT3lwVmRJWmRWQVptNWprZWZzNVJFSHR2ZXI3Sm91TDBwR0p6WDFhcGVmYkF2TlkvTHoKeXJNR2RyQjRVN2ZOUEFFTGRua2M5WlN3WG84ZkhPVjUvd0cvcHdJREFRQUJBb0lCQVFDeWN5aVpKenZJdm1UVgpTYzFYWWpHT1FieUZ3REk4TmZhUGZLT1pxWFhucVFpSkZMa283V0RGZVJlL09IOHlybVFVaXVqSE9KUERXUjJtCkhtaWVQS2l6eVdlWlFNb0dyS1hmeUZleWIvVHkyQUwzZkd0M3E5QlZTSGNIRkt5WHhvM0dYMXkxOWJ3V0I4M2UKaUFjUHcxZkRLTDd5T2pLTStiQ1hjek11Q1N3L0k5UHRIRlorb2RjT3piZkoxcnFpWmZ0T3Z0S0xhUmxJYkFyQwo2YVRzWDg3U085SmNPd0M0WWQ4TlNzbVdVV2dDUnl5V0g2MDR6S1MzaXpEMnZCeGxkMkNRYTUxdDk2aXZ4WEJUCm5Fc0xvL1U0VTNPb3FvR1JMa2ZHUGRVV0pjS1FWWk5QNFJxc3JtODBhdzloSXQ5SzVoMWZhSk9VSW9aWGdQc2IKckJsdExUSkJBb0dCQVBsQURVV3l1a1orcis5bmNuN0ZTeUhYZTB1Ukt0NHJaR2ZqUUZuSTlpNTJyUkZBK3hDeQpXWktHRTE1bE90eTUrUW5tblp0RlQ5bThmbzE3U1pzRHczUjByZE01aCtmbVVmSW82TzhnTlBPaHBqVG1JK2paCjJGSnZORy9QdVI0OThJUWRPVDFROVZpUjBLVDV2RjgzZnpucGVoa0QxaGxpOURqWENCbWVpS2REQW9HQkFNcVoKdGJ0R01oOVZBSDBnK3pIV0RQQmVkaVUzTnRDU3d6RzYxUVJnTlVSUUtLL2MxYmlJUjBmOXlSc1FrYkJNa01neQpsNm1BVTc3aDdISGwrTlgyMFNPR0NPajBod0VTN1pGQkpkTGQxcmgwd2lDMzZ5ZlJZdzlNbnc4c2JBOU1tNUJwCmJCNGRadXlsbE5EWXBIdUluSmFoZEdGbDR4ZGZEdG1UVUdzQ3VvWE5Bb0dCQVBDYi9YUDMrY3dOaGdja3BLbHUKQ1g1TXhuQUhYZ3VFZGZPM3M2bzR6alhDU0lXc1pmRVRTbGFnNlZlcGZ5NE90VEx5bGRpTEMyOVVnQkpTSHBidgpCaUtJZERqQWc4ckFVd0RpcnlJUHhDNGdNUk5GeXdxQUtVeENuNnNFS2w3Z1Npc3ZEczk3Tkt6Z2JqcWovazBvCmhML0IvQ1RqeUMxUGtoVEF0OGdMZXoyREFvR0FiVHBqdlJ6Q1J4d24wRTVvdjF5Y3l6YlNVWXdzRkZVYndZTHQKOTViN0FPS0tuUTNkZDhpWlRabm93NXV5UUM5M1cwZlRkb0lHSklKZlhLVFUvRjQrTVAzQmJmN3Zqa2VySjBYTgpZNXRRZVZBUm5Wd3EvdU9ubVljQXRraUgxZFBDaHlBZk56azQxVnNNR1hERGRRcVpDYWp3T3RhWWtYQWhiSEk4CnlBVEhsUDBDZ1lCYXRocnVzMlZMRnJ6c3psRGZpM1ZzbnVGOThYWDNnYW5IQnVkTUp5bzY3Y0t5NC8xQ3lVTEUKZTdEUUoxV2N4MlVicGZTWEx5eXREb1pPYWFCK2NscjhrTE9HSGNIclZrVVd1WXVtOEJHYS9DNVZjQTdIQUtTUwovMjFjRVg2QkFsc3lFMEpDWTdTZmpiTjNQZ1p2V1FIQXJ2RTk1ajdiVzRSNEpOYUVpOEJZR1E9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo");
            // config.setMasterUrl("");
            KubernetesClient client = new KubernetesClientBuilder().withConfig(config).build();
            System.out.println(client.getMasterUrl());
            // jobName:seatun01-2,namespace:flinktest
            if (Boolean.TRUE.equals(pytorchJobExist(client, "abcd-19", "dolphinscheduler"))) {
                System.out.println("aaaaa exist");

            } else {
                System.out.println("nnnnnnnnnnn 不存在");
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
        String yml = "---\n"
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

    public static Boolean pytorchJobExist(KubernetesClient client, String jobName, String namespace) {
        Optional<GenericKubernetesResource> result;
        try {

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

    private static CustomResourceDefinitionContext getPytorchCustomResourceDefinitionContext() {
        return new CustomResourceDefinitionContext.Builder()
                .withGroup("kubeflow.org")
                .withVersion("v1")
                .withScope("Namespaced")
                .withPlural("pytorchjobs")
                .withKind("PyTorchJob")
                .build();
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

        // 监控任务的运行

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
        // component=jobmanager,dolphinscheduler-label=6_6
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
