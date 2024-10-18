package org.apache.dolphinscheduler.api.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OssFile {

    /**
     * 本地文件路径
     */
    private String localFilePath;

    /**
     * OSS 存储时文件路径(桶下文件所属的文件夹)
     */
    private String path;

    /**
     * 原始文件名
     */
    private String originalFileName;

    /**
     * 存储桶
     */
    private String bucketName;

    /**
     * url
     */
    private String url;
}
