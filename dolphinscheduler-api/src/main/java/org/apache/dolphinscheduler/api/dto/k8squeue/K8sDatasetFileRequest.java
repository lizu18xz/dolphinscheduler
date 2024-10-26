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
public class K8sDatasetFileRequest {

    /**
     * id
     */
    private Integer id;

    /**
     * 队列名称
     */
    private String name;

    private Long code;

    private String flowName;

    private String taskName;

    private String taskType;

    /**
     * 动态更新最新的
     */
    private int taskInstanceId;

    private String status;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;

}
