package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class FetchVolumeResponse {

    private String type;

    private String id;

    //源
    private String fileUrl;

    //目标
    private String downAddr;

    private String host;

    private String appKey;

    private String appSecret;

    private String bucketName;

    private String projectIds;

}
