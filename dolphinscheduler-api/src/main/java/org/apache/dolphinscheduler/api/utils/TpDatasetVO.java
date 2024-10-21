package org.apache.dolphinscheduler.api.utils;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TpDatasetVO {
    @Schema(description = "*数据集id")
    private Long tpDatasetId;

    @Schema(description = "*对象存储中存储桶下的文件相对路径名集合,每个map接收两个数据如objectKey:a,size:10")
    private List<Map<String,Object>> relativePathList;

    @Schema(description = "*数据集版本id")
    private Long datasetVersionId;

    /**
     * 工作流id
     */
    private Long workFlowId;
}
