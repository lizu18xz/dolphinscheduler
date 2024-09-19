package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_ds_k8s_queue")
public class K8sQueue {
    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;
    /**
     * 队列名称
     */
    private String name;

    private Integer weight;

    private Double capabilityCpu;

    private Double capabilityMemory;

    private Boolean reclaimable;

    private String state;

    /**
     * 所属集群配置
     * */
    private Long clusterCode;


}
