package org.apache.dolphinscheduler.api.dto.k8squeue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class K8sQueueTaskRequest {

    /**
     * 队列名称
     */
    private String name;

    private String projectName;

    private Long code;

    private String flowName;

    private String taskName;

    private String taskType;

    private int priority;

    private String taskResourceInfo;

}
