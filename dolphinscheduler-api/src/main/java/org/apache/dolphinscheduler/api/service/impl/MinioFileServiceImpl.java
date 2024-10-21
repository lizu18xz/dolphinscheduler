package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.service.MinioFileService;
import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;


@Service
public class MinioFileServiceImpl implements MinioFileService {

    @Resource
    MinioSdFileServiceImpl minioSdFileService;


    public Boolean localFileMultipartUpload(FileCustomUploadVO fileCustomUploadVO) {
        return minioSdFileService.localFolderMultipartUpload(fileCustomUploadVO);
    }


    @Override
    public String getSdSignedUrl(String fileName, Integer expireSeconds, String bucketName,String host,String  key,String appSecret) {
        return minioSdFileService.getSdSignedUrl(fileName, expireSeconds, bucketName,host,key,appSecret);
    }
}
