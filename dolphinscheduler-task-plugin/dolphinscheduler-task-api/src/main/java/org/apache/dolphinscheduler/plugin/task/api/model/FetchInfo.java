package org.apache.dolphinscheduler.plugin.task.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FetchInfo implements Serializable {

    private String dirId;

    private String fetchId;

    private String fetchName;
    /**
     * 类型是local/minio/...
     */
    private String fetchType;

    /**
     * 节点的输入输出挂载
     */
    private String fetchDataVolume;

    /**
     * 拉取数据的参数
     */
    private String fetchDataVolumeArgs;

}
