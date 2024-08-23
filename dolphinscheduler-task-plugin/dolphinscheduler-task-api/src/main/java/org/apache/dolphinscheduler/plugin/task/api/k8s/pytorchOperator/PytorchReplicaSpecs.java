package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 具体细节配置
 */
@Data
public class PytorchReplicaSpecs {

    @JsonProperty("Master")
    private Master Master;

    @JsonProperty("Worker")
    private Worker Worker;

}
