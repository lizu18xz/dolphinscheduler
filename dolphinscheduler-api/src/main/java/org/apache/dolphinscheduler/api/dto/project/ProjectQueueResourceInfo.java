package org.apache.dolphinscheduler.api.dto.project;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class ProjectQueueResourceInfo {

    private Double allocatedCpu;

    private Double allocatedMemory;

    private String highGpuName;

    private Integer highAllocatedGpu;

    private String lowGpuName;

    private Integer lowAllocatedGpu;

}
