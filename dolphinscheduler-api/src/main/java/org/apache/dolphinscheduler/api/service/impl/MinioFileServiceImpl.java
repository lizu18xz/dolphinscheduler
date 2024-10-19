package org.apache.dolphinscheduler.api.service.impl;

import org.apache.dolphinscheduler.api.service.MinioFileService;
import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;


@Service
public class MinioFileServiceImpl implements MinioFileService {

    @Resource
    MinioSdFileServiceImpl minioSdFileService;



    /**
     * 根据传入的本地文件路径分段上传
     *
     * @param path          存储在服务器的哪个路径下(不包含文件名, 文件名会从localFilePath中读取最终拼上该路径上传到文件服务器)
     * @param localFilePath 本地文件路径
     * @return CommonResult<String> url
     */
    public Boolean localFileMultipartUpload(FileCustomUploadVO fileCustomUploadVO) {
        return minioSdFileService.localFolderMultipartUpload(fileCustomUploadVO);
    }

//    /**
//     * 获取临时签名的url
//     *
//     * @param objectKey
//     * @param expireSeconds 有效时长(单位秒), 传null默认7天
//     * @return
//     */
//    public String getSignedUrl(String objectKey, Integer expireSeconds, String bucket) {
//            return minioSdFileService.getSignedUrl(objectKey, expireSeconds, bucket);
//    }
//
//    /**
//     * 获取临时签名的url集合
//     *
//     * @param objectKeyList
//     * @return
//     */
//    public Map<String, String> getSignedUrlList(List<String> objectKeyList, String bucket) {
//            return minioSdFileService.getSignedUrlList(objectKeyList, bucket);
//     }


    @Override
    public String getSdSignedUrl(String fileName, Integer expireSeconds, String bucketName,String host,String  key,String appSecret) {
        return minioSdFileService.getSdSignedUrl(fileName, expireSeconds, bucketName,host,key,appSecret);
    }
}
