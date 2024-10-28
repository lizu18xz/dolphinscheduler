package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class DataSetResponse {

    //来源名称
    private String name;

    private String type;

    private String id;

    private String host;

    private String appKey;

    private String appSecret;

    private String bucketName;

    //存储中的路径
    private String relativePath ;

    private String projectIds;

    private String tpDatasetId;

    private List<DataSetResponse> children;

}
