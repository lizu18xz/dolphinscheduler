package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;

import java.util.List;
import java.util.Map;

public interface MinioFileService {


//    /**
//     * 根据传入的本地文件路径分段上传
//     * @param path 存储在服务器的哪个路径下(不包含文件名, 文件名会从localFilePath中读取最终拼上该路径上传到文件服务器)
//     * @param localFilePath 本地文件路径
//     * @return CommonResult<String> url
//     */
Boolean  localFileMultipartUpload(FileCustomUploadVO fileCustomUploadVO);
//    /**
//     * 获取临时签名的url
//     * @param objectKey
//     * @param expireSeconds  有效时长(单位秒), 传null默认7天
//     * @return
//     */
//    String getSignedUrl(String objectKey,Integer expireSeconds, String bucket);
//
//    /**
//     * 获取临时签名的url集合
//     * @param objectKeyList
//     * @return
//     */
//    Map<String,String> getSignedUrlList(List<String> objectKeyList, String bucket);
//


    String getSdSignedUrl(String fileName, Integer expireSeconds, String bucketName,String host,String  key,String appSecret);


}
