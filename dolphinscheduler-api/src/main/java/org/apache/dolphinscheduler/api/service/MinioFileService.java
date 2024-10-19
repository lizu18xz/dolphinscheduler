package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;

import java.util.List;
import java.util.Map;

public interface MinioFileService {



    Boolean localFileMultipartUpload(FileCustomUploadVO fileCustomUploadVO);


    String getSdSignedUrl(String fileName, Integer expireSeconds, String bucketName, String host, String key, String appSecret);


}
