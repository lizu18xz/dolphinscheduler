package org.apache.dolphinscheduler.plugin.task.api.k8s.spark;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;

/**
 * @author lizu
 * @since 2022/5/1
 */

public class SparkGenericKubernetesResource extends GenericKubernetesResource {

    private SparkOperatorSpec spec;

    public SparkOperatorSpec getSpec() {
        return spec;
    }

    public void setSpec(SparkOperatorSpec spec) {
        this.spec = spec;
    }
}
