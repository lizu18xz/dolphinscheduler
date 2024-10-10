package org.apache.dolphinscheduler.api.dto.k8squeue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.dolphinscheduler.api.dto.project.ProjectQueueResourceInfo;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class K8sQueueRequest {

    private String name;

    private String projectName;

    private String projectEnName;

    private Integer weight;

    private Boolean reclaimable;

    private Long clusterCode;

    private ProjectQueueResourceInfo projectQueueResourceInfo;

}
