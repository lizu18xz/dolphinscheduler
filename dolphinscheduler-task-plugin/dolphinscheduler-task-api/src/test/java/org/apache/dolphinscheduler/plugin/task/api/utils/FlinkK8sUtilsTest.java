package org.apache.dolphinscheduler.plugin.task.api.utils;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.internal.SerializationUtils;
import java.io.InputStream;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flink.FlinkDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lizu
 * @since 2022/7/29
 */
public class FlinkK8sUtilsTest {

    private static final String TEST_NAMESPACE = "flink-operator-test";
    private static final String SERVICE_ACCOUNT = "flink-operator";
    private static final String CLUSTER_ROLE_BINDING = "flink-operator-cluster-role-binding";
    private static final String FLINK_VERSION = "1.15.1";
    private static final String IMAGE = String.format("flink:%s", FLINK_VERSION);
    private static final Logger LOG = LoggerFactory.getLogger(FlinkK8sUtilsTest.class);
    private KubernetesClient client;


    public static void main(String[] args) {

        K8sUtils k8sUtils = new K8sUtils();
        k8sUtils.buildNoAuthClient("https://kubernetes.docker.internal:6443");

        k8sUtils.createFlinkOperatorJob("flink-operator",getFlinkDeployment());

        //k8sUtils.deleteFlinkOperatorJob("flink-operator",getFlinkDeployment());

        Boolean aBoolean = k8sUtils.flinkOperatorJobExist("pod-template-example", "flink-operator");
        System.out.println(aBoolean);
    }


    public static FlinkDeployment getFlinkDeployment() {
        InputStream resourceAsStream = FlinkK8sUtilsTest.class
            .getResourceAsStream("/pod-template.yaml");
        try {
            FlinkDeployment flinkDeployment = SerializationUtils.getMapper()
                .readValue(resourceAsStream, FlinkDeployment.class);
            return flinkDeployment;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


}

