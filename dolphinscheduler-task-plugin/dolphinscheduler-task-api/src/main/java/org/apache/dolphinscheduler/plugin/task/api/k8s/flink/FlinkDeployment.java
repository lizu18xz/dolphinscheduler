package org.apache.dolphinscheduler.plugin.task.api.k8s.flink;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

/**
 * @author lizu
 * @since 2022/7/29
 */
public class FlinkDeployment  extends GenericKubernetesResource {

    private FlinkDeploymentSpec spec;

    public FlinkDeploymentSpec getSpec() {
        return spec;
    }

    public void setSpec(FlinkDeploymentSpec spec) {
        this.spec = spec;
    }
}
