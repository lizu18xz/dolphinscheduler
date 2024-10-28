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

    private String name;

    /**
     * 动态更新最新的
     */
    private int taskInstanceId;

    private int processInstanceId;

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
