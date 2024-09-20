package org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import lombok.Data;

import java.util.List;

@Data
public class Tasks {

    private Integer replicas;

    private String name;

    private List<Policies> policies;

    private PodTemplateSpec template;

}
