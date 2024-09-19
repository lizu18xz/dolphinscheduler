package org.apache.dolphinscheduler.api.dto.k8squeue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class K8sQueueRequest {

    private Long clusterCode;

    private String name;

    private Integer weight;

    private Double capabilityCpu;

    private Double capabilityMemory;

    private Boolean reclaimable;

}
