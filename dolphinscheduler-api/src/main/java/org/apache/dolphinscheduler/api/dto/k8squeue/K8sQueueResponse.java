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
public class K8sQueueResponse {

    private Long clusterCode;

    private String name;

    private String queue;

    private Boolean reclaimable;

    private String projectName;

    private Integer weight;

    /**
     * 资源信息
     */
    private String resourceInfo;


    private String state;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;
}
