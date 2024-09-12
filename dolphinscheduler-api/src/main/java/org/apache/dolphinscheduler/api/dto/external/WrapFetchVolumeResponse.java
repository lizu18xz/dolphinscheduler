package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class WrapFetchVolumeResponse {

    /**
     * 类型是local/minio/...
     * */
    private String fetchType;

    /**
     * 节点的输入输出挂载
     */
    private String fetchDataVolume;

    /**
     * 拉取数据的参数
     * */
    private String fetchDataVolumeArgs;


}
