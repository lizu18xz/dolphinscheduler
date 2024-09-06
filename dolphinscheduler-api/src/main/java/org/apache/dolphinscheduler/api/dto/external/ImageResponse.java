package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class ImageResponse {

    private Integer projectId;

    private String url;

    private Integer repositoryId;

    private Integer tagId;

    private String imageName;


}
