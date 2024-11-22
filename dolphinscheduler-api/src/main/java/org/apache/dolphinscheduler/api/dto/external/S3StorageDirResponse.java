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
public class S3StorageDirResponse {

    private String settingName;

    private String endpoint;

    private String accessKey;

    private String secretKey;

    private String bucketName;

    private String type;

    private String fileUrl;
}
