package org.apache.dolphinscheduler.plugin.task.api.utils;

import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import java.util.HashMap;
import java.util.Map;
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
import org.junit.Test;

/**
 * @author lizu
 * @since 2022/5/6
 */
public class K8sUtilsTest {

    @Test
    public void testGetJdbcInfo() {

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
        Map<String, String> envVars = new HashMap<>();
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
