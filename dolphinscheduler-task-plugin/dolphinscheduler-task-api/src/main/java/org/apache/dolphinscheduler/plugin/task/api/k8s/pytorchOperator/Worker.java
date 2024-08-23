package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class Worker {

    private Integer replicas;

    private String restartPolicy;

    private PodTemplateSpec template;

}
