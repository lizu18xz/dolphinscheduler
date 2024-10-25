package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("t_ds_k8s_queue_task")
public class K8sQueueTask {
    /**
     * id
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    /**
     * 队列名称
     */
    private String name;

    private String projectName;

    private Long code;

    private String flowName;

    private String taskName;

    private String taskType;

    private String nameSpace;

    /**
     * 动态更新最新的
     * */
    private int taskInstanceId;

    private int priority;

    private String taskResourceInfo;

    private String taskStatus;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;
}