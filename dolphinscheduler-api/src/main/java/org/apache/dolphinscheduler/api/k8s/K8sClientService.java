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

package org.apache.dolphinscheduler.api.k8s;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import org.apache.dolphinscheduler.dao.entity.K8sNamespace;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;

import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Encapsulates all client-related operations, not involving the db
 */
@Component
public class K8sClientService {

    private static Yaml yaml = new Yaml();
    @Autowired
    private K8sManager k8sManager;

    public ResourceQuota upsertNamespaceAndResourceToK8s(K8sNamespace k8sNamespace,
                                                         String yamlStr) {
        if (!checkNamespaceToK8s(k8sNamespace.getNamespace(), k8sNamespace.getClusterCode())) {
            throw new RuntimeException(String.format(
                    "namespace %s does not exist in k8s cluster, please create namespace in k8s cluster first",
                    k8sNamespace.getNamespace()));
        }
        return upsertNamespacedResourceToK8s(k8sNamespace, yamlStr);
    }

    public Optional<Namespace> deleteNamespaceToK8s(String name, Long clusterCode) {
        Optional<Namespace> result = getNamespaceFromK8s(name, clusterCode);
        if (result.isPresent()) {
            KubernetesClient client = k8sManager.getK8sClient(clusterCode);
            Namespace body = new Namespace();
            ObjectMeta meta = new ObjectMeta();
            meta.setNamespace(name);
            meta.setName(name);
            body.setMetadata(meta);
            client.namespaces().delete(body);
        }
        return getNamespaceFromK8s(name, clusterCode);
    }

    private ResourceQuota upsertNamespacedResourceToK8s(K8sNamespace k8sNamespace,
                                                        String yamlStr) {

        KubernetesClient client = k8sManager.getK8sClient(k8sNamespace.getClusterCode());

        // 创建资源
        ResourceQuota queryExist = client.resourceQuotas()
                .inNamespace(k8sNamespace.getNamespace())
                .withName(k8sNamespace.getNamespace())
                .get();

        ResourceQuota body = yaml.loadAs(yamlStr, ResourceQuota.class);

        if (queryExist != null) {
            if (k8sNamespace.getLimitsCpu() == null && k8sNamespace.getLimitsMemory() == null) {
                client.resourceQuotas().inNamespace(k8sNamespace.getNamespace())
                        .withName(k8sNamespace.getNamespace())
                        .delete();
                return null;
            }
        }

        return client.resourceQuotas().inNamespace(k8sNamespace.getNamespace())
                .withName(k8sNamespace.getNamespace())
                .createOrReplace(body);
    }

    private Optional<Namespace> getNamespaceFromK8s(String name, Long clusterCode) {
        NamespaceList listNamespace =
                k8sManager.getK8sClient(clusterCode).namespaces().list();

        Optional<Namespace> list =
                listNamespace.getItems().stream()
                        .filter((Namespace namespace) -> namespace.getMetadata().getName().equals(name))
                        .findFirst();

        return list;
    }

    private boolean checkNamespaceToK8s(String name, Long clusterCode) {
        Optional<Namespace> result = getNamespaceFromK8s(name, clusterCode);
        return result.isPresent();
    }


    public void loadApplyYmlJob(String yaml, Long clusterCode) {
        try {
            KubernetesClient client = k8sManager.getK8sClient(clusterCode);
            client.load(new ByteArrayInputStream((yaml).getBytes())).createOrReplace();
        } catch (Exception e) {
            throw new TaskException("fail to create yml", e);
        }
    }

    public void deleteApplyYmlJob(String yaml, Long clusterCode) {
        try {
            KubernetesClient client = k8sManager.getK8sClient(clusterCode);
            client
                    .load(new ByteArrayInputStream((yaml).getBytes()))
                    .delete();
        } catch (Exception e) {
            throw new TaskException("fail to delete yml", e);
        }
    }


    public List<GenericKubernetesResource> getVcJobStatus(String namespace, Long clusterCode) {
        try {
            KubernetesClient client = k8sManager.getK8sClient(clusterCode);
            CustomResourceDefinitionContext context = getQueueJobCustomResourceDefinitionContext();
            MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> resourceMixedOperation =
                    client.genericKubernetesResources(context);
            GenericKubernetesResourceList list = resourceMixedOperation
                    .inNamespace(namespace).list();
            List<GenericKubernetesResource> items = list.getItems();
            return items;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TaskException("fail to check queue job: ", e);
        }

    }


    private CustomResourceDefinitionContext getQueueJobCustomResourceDefinitionContext() {
        return new CustomResourceDefinitionContext.Builder()
                .withGroup("batch.volcano.sh")
                .withVersion("v1alpha1")
                .withScope("Namespaced")
                .withPlural("jobs")
                .withKind("Job")
                .build();
    }


}
