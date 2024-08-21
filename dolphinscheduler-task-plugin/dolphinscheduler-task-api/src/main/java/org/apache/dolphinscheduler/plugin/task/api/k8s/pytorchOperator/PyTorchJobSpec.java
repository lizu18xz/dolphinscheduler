package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import lombok.Data;

@Data
public class PyTorchJobSpec {

    private PytorchReplicaSpecs pytorchReplicaSpecs;

}
