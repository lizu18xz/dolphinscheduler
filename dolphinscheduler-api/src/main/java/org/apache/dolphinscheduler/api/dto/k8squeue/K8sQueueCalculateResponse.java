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
public class K8sQueueCalculateResponse {

    /**
     * 名称
     * */
    private String name;

    /**
     * 总量
     * */
    private String total;

    private String use;

}
