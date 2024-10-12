package org.apache.dolphinscheduler.api.dto.k8squeue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

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

    /**
     * 队列资源信息
     */
    private String resourceInfo;

    /**
     * 当前task资源信息
     */
    private String taskResourceInfo;

    private String taskStatus;

    private Date updateTime;


}
