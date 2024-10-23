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
public class TreeResponse {

    private String id;

    private Long parentId;

    private String name;

    private String weight;

    private String bucketName;

    private String filePath;

    private String host;

    private String appKey;

    private String appSecret;

    private List<TreeResponse> children;
}
