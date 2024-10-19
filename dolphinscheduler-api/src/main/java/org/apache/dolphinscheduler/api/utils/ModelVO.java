package org.apache.dolphinscheduler.api.utils;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.io.Serializable;
@Data
public class ModelVO implements Serializable {
    @Schema(description = "项目名称")
    private String projectName;

    @Schema(description = "模型名称")
    private String modelName;

    @Schema(description = "模型版本")
    private String modelVersion;

    @Schema(description = "训练框架")
    private String modelTrainFramework;

    @Schema(description = "推理框架")
    private String reasonFramework;

    @Schema(description = "模型文件")
    private String modelFile;

    @Schema(description = "模型描述")
    private String modelDesc;
}
