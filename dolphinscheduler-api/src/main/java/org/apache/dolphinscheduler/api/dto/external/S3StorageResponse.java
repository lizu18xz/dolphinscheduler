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
public class S3StorageResponse {

    private String bucketName;

    private String host;

    private String appKey;

    private String appSecret;

    private List<String> folderList;

    private List<String> fileList;

}
