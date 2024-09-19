package org.apache.dolphinscheduler.api.dto.k8squeue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class K8sQueueTaskResponse {
    /**
     * id
     */
    private Integer id;

    /**
     * 队列名称
     */
    private String name;

    private String projectName;

    private String flowName;

    private String taskName;

    private int priority;

    private String state;

    private Boolean share;

    private Double cpu;

    private Double memory;

}
