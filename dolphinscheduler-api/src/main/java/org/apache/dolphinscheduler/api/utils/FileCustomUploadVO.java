package org.apache.dolphinscheduler.api.utils;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.io.Serializable;

@Schema(description = "管理后台 - 自定义上传客户端实现上传文件 Request VO")
@Data
public class FileCustomUploadVO implements Serializable {

    private String localFilePath;

    /**
     * 数据集id
     */
    private Long dataId;

    /**
     * 桶名称
     */
    private String bucketName;

    /**
     * minio地址
     */
    private String host;

    /**
     * 对象存储key
     */
    private String key;

    /**
     * 对象存储secret
     */
    private String appSecret;

    /**
     * 项目名称
     */
    private String projectName;

    /**
     * 项目名称
     */
    private Long projectId;

    /**
     * 对象存储前缀路径（比如说桶的前缀路径） 输出文件路径
     */
    private String path;

    /**
     * 0-训练平台 1-数据集处理
     */
    private int type;
    /**
     * 模型id
     */
    private Long modelId;

    /**
     * 工作流id
     */
    private Long workFlowId;

}
