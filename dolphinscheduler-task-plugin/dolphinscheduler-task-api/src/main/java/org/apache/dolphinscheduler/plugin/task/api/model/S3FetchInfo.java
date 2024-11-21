package org.apache.dolphinscheduler.plugin.task.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class S3FetchInfo implements Serializable {

    private String bucketName;

    private String host;

    private String appKey;

    private String appSecret;

    private String path;

}
