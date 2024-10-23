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
public class FileResponse {

    private String type;

    private String id;

    private Long userId;

    private Long tenantId;

    private String bucketName;

    private String fileName;

    private String relativePath;

    private String host;

    private String appKey;

    private String appSecret;

}
