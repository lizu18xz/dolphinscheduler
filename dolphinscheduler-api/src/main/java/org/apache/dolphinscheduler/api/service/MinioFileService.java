package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;

public interface MinioFileService {



    Boolean localFileMultipartUpload(FileCustomUploadVO fileCustomUploadVO);


    String getSdSignedUrl(String fileName, Integer expireSeconds, String bucketName, String host, String key, String appSecret);


}
