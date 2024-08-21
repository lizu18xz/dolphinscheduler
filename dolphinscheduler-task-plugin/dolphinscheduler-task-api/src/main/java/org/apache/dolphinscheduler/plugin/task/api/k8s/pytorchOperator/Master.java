package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class Master {

    private Integer replicas;

    private String restartPolicy;

    private PodTemplateSpec template;


}
