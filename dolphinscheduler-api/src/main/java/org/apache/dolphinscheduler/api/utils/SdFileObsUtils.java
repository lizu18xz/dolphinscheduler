package org.apache.dolphinscheduler.api.utils;

import cn.hutool.core.date.DateUtil;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.util.*;

/**
 * 单例模式 工具类型、
 */
@Component
@Slf4j
@RefreshScope
@SuppressWarnings("all")
public class SdFileObsUtils {
    /**
     * Description  获取 MinioClient 对象
     * Create By Mr.Fang
     *
     * @return com.ming.utils.MinioUtils
     * @time 2022/7/10 10:18
     **/
    public ObsClient obsClient(String host,String appkey,String appSecret) {
        return new ObsClient(appkey, appSecret, host);
    }


    /**
     * 上传文件到OBS
     */
    public String uploadFile(String bucketName, MultipartFile file, String prefixPath,String host,String appkey,String appSecret) throws IOException {
        String fileName = file.getOriginalFilename();
        if (org.springframework.util.StringUtils.hasText(prefixPath)) {
            fileName = prefixPath + "/" + fileName;
        }
        PutObjectResult result = obsClient(host,appkey,appSecret).putObject(bucketName, fileName, file.getInputStream());
        return result.getObjectUrl();
    }




    /**
     * 根据本地文件路径分片上传
     *
     * @param bucketName    桶名
     * @param objectKey     最终obs中的文件名
     * @param localFilePath 本地文件路径
     * @return url
     * @throws IOException
     */
    @SuppressWarnings("all")
    public Boolean localFileMultipartUpload(String bucketName, String objectKey, String localFilePath,String host,String  key,String appSecret)  {
        String resultUrl = "";
        File file = new File(localFilePath);
        long contentLength = file.length();
        long partSize = 5 * 1024 * 1024; // Set part size to 5 MB.

        List<PartEtag> partETags = new ArrayList<>();

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectKey);
        InitiateMultipartUploadResult initResponse = obsClient(host,key,appSecret).initiateMultipartUpload(initRequest);
        try {
            FileInputStream fis = new FileInputStream(file);
            try {

                long filePosition = 0;
                for (int i = 1; filePosition < contentLength; i++) {
                    partSize = Math.min(partSize, (contentLength - filePosition));
                    UploadPartRequest uploadRequest = new UploadPartRequest(bucketName, objectKey);
                    uploadRequest.setUploadId(initResponse.getUploadId());
                    uploadRequest.setPartNumber(i);
                    uploadRequest.setInput(new FileInputStream(file));
                    uploadRequest.setPartSize(partSize);
                    UploadPartResult uploadResult = obsClient(host,key,appSecret).uploadPart(uploadRequest);
                    partETags.add(new PartEtag(uploadResult.getEtag(), uploadResult.getPartNumber()));

                    filePosition += partSize;
                }

                // 合并操作
                CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest();
                compRequest.setBucketName(bucketName);
                compRequest.setObjectKey(objectKey);
                compRequest.setUploadId(initResponse.getUploadId());
                compRequest.setPartEtag(partETags);
                CompleteMultipartUploadResult result = obsClient(host,key,appSecret).completeMultipartUpload(compRequest);
//            resultUrl = result.getObjectUrl();
                log.info("Upload complete.");
            } catch (Exception e) {
            /*AbortMultipartUploadRequest: 用于构造一个请求对象，包含必要的信息来中止分段上传操作。需要提供桶名（bucket name）、对象键（object key）以及上传ID（upload ID）。
            obsClient.abortMultipartUpload: 调用此方法来中止上传操作，并删除已经上传的所有部分，释放存储空间。
            上传失败处理: 在分段上传过程中，如果任何部分上传失败，都应该调用abortMultipartUpload来中止上传会话，避免资源浪费。
            清理部分数据: 中止上传会删除已经上传的部分数据，确保不会有残留的数据占用存储空间。
            异常处理: 在异常处理中调用abortMultipartUpload来确保分段上传在任何失败情况下都能被正确处理。
            */
                obsClient(host,key,appSecret).abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, objectKey, initResponse.getUploadId()));
                log.info("Upload failed.");
                e.printStackTrace();
                return false;
            } finally {
                fis.close();
            }
        }catch (Exception e){
            log.error("Upload failed."+e.getMessage());
        }

        return true;
    }



    /**
     * 获取临时签名的url
     *
     * @param bucketName
     * @param objectKey
     * @return
     */
    public String getSignedUrl(String bucketName, String objectKey, String host,String  key,String appSecret) {
        Integer  expireSeconds = 604800;
        String url = "";
        try {
            // 获取图片转码的下载链接
            // URL有效期，默认604800秒,7天
            if (expireSeconds == null || expireSeconds <= 0) {
                expireSeconds = 604800;
            } else {
                // 最大20年防止设置太大报错
                if (expireSeconds > 3600 * 24 * 365 * 20) {
                    expireSeconds = 3600 * 24 * 365 * 20;
                }
            }

            TemporarySignatureRequest request = new TemporarySignatureRequest(HttpMethodEnum.GET, expireSeconds);
            request.setBucketName(bucketName);
            request.setObjectKey(objectKey);
            // 设置图片转码参数
            Map<String, Object> queryParams = new HashMap<String, Object>();
            /*queryParams.put("x-image-process", "image/resize,m_fixed,w_100,h_100/rotate,100");
            request.setQueryParams(queryParams);*/
            TemporarySignatureResponse response = obsClient(host,key,appSecret).createTemporarySignature(request);
            // 获取支持图片转码的下载链接
            System.out.println("Getting object using temporary signature url:");
            System.out.println("SignedUrl:" + response.getSignedUrl());
            url = response.getSignedUrl();
        } catch (ObsException e) {
            System.out.println("getObject failed");
            // 请求失败,打印http状态码
            System.out.println("HTTP Code:" + e.getResponseCode());
            // 请求失败,打印服务端错误码
            System.out.println("Error Code:" + e.getErrorCode());
            // 请求失败,打印详细错误信息
            System.out.println("Error Message:" + e.getErrorMessage());
            // 请求失败,打印请求id
            System.out.println("Request ID:" + e.getErrorRequestId());
            System.out.println("Host ID:" + e.getErrorHostId());
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("getObject failed");
            // 其他异常信息打印
            e.printStackTrace();
        }
        return url;
    }
    /**
     * @param fileName
     * @return
     * @author xhw
     * @生成文件名
     */
    public String getUUIDFileName(String fileName) {
        int idx = fileName.lastIndexOf(".");
        String extention = fileName.substring(idx);
        String newFileName = DateUtil.format(new Date(), "yyyyMMddHHmmssSSS") + "_" + UUID.randomUUID().toString().replace("-", "") + extention;
        return newFileName;
    }

}



