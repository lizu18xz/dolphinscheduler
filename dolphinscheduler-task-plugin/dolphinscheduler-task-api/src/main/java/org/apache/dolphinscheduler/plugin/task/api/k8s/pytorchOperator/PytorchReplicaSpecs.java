package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

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
