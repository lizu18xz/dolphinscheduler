package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class S3StorageRequest {

    private String projectName;

    private String prefix;

    private String endpoint;

    private String accessKey;

    private String secretKey;

    private String bucketName;

}
