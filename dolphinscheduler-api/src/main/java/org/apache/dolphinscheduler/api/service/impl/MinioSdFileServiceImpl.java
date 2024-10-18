package org.apache.dolphinscheduler.api.service.impl;

import cn.hutool.core.collection.CollUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;
import org.apache.dolphinscheduler.api.utils.SdFileMinioUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


@Service
@Slf4j
public class MinioSdFileServiceImpl {

    @Autowired
    private SdFileMinioUtils minioUtils;
    public List<Map<String, String>> localFolderMultipartUpload(FileCustomUploadVO fileCustom) {
        List<Map<String, String>> uploadResponses = new ArrayList<>();
        File folder = new File(fileCustom.getLocalFilePath());

        // 检查文件夹是否存在
        if (!folder.exists() || !folder.isDirectory()) {
            System.out.println("指定的路径不存在或不是一个文件夹: " + fileCustom.getLocalFilePath());
            return uploadResponses;
        }

        // 递归遍历文件夹
        uploadFilesFromFolder(folder, fileCustom, uploadResponses);

        return uploadResponses; // 返回上传成功的文件信息
    }

    private void uploadFilesFromFolder(File folder, FileCustomUploadVO fileCustom, List<Map<String, String>> uploadResponses) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) { // 确保是文件而不是子文件夹
                    String objectKey = file.getName(); // 使用文件名作为对象的 key
                    String path = null;
                    if (fileCustom.getType() == 0) {
                        path = fileCustom.getDataName() + "/" + file.getAbsolutePath();
                    } else {
                        path = file.getAbsolutePath();
                    }
                    // 上传文件
                    Boolean uploadSuccess = minioUtils.uploadObject(fileCustom.getBucketName(), path, objectKey, fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());

                    if (uploadSuccess) {
                        // 构建文件的 URL
                        String fileUrl = minioUtils.getObjectUrl(fileCustom.getBucketName(), objectKey, fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());
                        Map<String, String> map = new HashMap<>(16);
                        map.put("key", path + "/" + objectKey);
                        map.put("url", fileUrl);
                        uploadResponses.add(map);
                    } else {
                        System.out.println("文件上传失败: " + file.getName());
                    }
                } else if (file.isDirectory()) { // 如果是目录，递归处理
                    uploadFilesFromFolder(file, fileCustom, uploadResponses);
                }
            }
        } else {
            System.out.println("文件夹 " + folder.getAbsolutePath() + " 为空");
        }
    }


//    public static void main(String[] args) {
//        FileCustomUploadVO fileCustom = new FileCustomUploadVO();
//        fileCustom.setHost("http://10.78.5.103:9991");
//        fileCustom.setKey("V0M2NPhCfk1exMSxAbnI");
//        fileCustom.setKey("5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX");
//        fileCustom.setType(1);
//        fileCustom.setBucketName("other-data");
//        fileCustom.setLocalFilePath("C:\\Users\\swh\\Desktop\\train_mnist");
//    }
//    public List<String> localFolderMultipartUpload(FileCustomUploadVO fileCustom) {
//
//        log.info("递归遍历的目录名为: {}", directoryPath);
//        List<String> filePaths = new ArrayList<>();
//        traverseDirectory(directoryPath, filePaths);
//        if (CollUtil.isNotEmpty(filePaths)) {
//            // 初始化 CountDownLatch
//            CountDownLatch latch = new CountDownLatch(filePaths.size());
//            Map<String, List> group = GroupUtil.groupList(filePaths, 50);
//            group.keySet().forEach(k->{
//                List<String> list = group.get(k);
//                taskExecutor.execute(() -> {
//                    list.stream().forEach(absPath -> {
//                        try {
//                            // 存储在minio中的文件名中"\\"替换"/",用来测试windows, linux不影响
//                            String absPathReplace1 = absPath.replace("\\", "/");
//                            // 替换回去minio中的文件名和gnss目录结构保持一致
//                            String absPathReplace2 = absPathReplace1.replace("/data/mnt/driverData", "/home/data_exchange");
//                            // 上传到minio
//                            Boolean b = uploadMinioObject(driverMinioConfig.getBucketName(), absPath, absPathReplace2);
//                            // 上传到obs
//                            // Boolean b = uploadObsObject(driverMinioConfig.getBucketName(), absPath, absPathReplace2);
//
//                        } catch (Exception e) {
//                            log.error("driver服务处理消息中的数据上传文件到minio出现错误,当前传的文件名为: {}",absPath,e);
//                        }finally {
//                            latch.countDown();
//                        }
//                    });
//                });
//            });
//            try {
//                latch.await();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        log.info(directoryPath + "文件夹下的数据已全部上传");
//        List<String> uploadedFileUrls = new ArrayList<>();
//        File folder = new File(fileCustom.getLocalFilePath());
//
//        if (!folder.exists() || !folder.isDirectory()) {
//            log.error("指定的路径不存在或不是一个文件夹: {}", fileCustom.getLocalFilePath());
//            return uploadedFileUrls; // 返回空列表
//        }
//
//        // 获取文件夹中的所有文件
//        File[] files = folder.listFiles();
//        if (files != null) {
//            for (File file : files) {
//                if (file.isFile()) { // 确保是文件而不是子文件夹
//                    try {
//                        FileCustomUploadVO fileCustomUpload = new FileCustomUploadVO();
//                        fileCustomUpload.setLocalFilePath(file.getAbsolutePath());
//                        fileCustomUpload.setBucket(fileCustom.getBucket());
//                        fileCustomUpload.setHost(fileCustom.getHost());
//                        fileCustomUpload.setKey(fileCustom.getKey());
//                        fileCustomUpload.setAppSecret(fileCustom.getAppSecret());
//                        String path = fileCustom.getDataName()+"/"+file.getAbsolutePath();
//
//                        minioUtils.uploadObject(fileCustom.getBucket(),path,);
//                        // 上传文件
//                        OssFile ossFileMerge = minioUtils.putChunkObject(fileCustomUpload);
//                        String originBucketName = ossFileMerge.getBucketName();
//                        String objectName = file.getName(); // 使用文件名作为对象名称
//
//                        // 合并文件
//                        ossFileMerge = minioUtils.composeObject(originBucketName, fileCustom.getBucket(), objectName,
//                                fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());
//                        log.info("文件 {} 上传成功，合并到桶：{}", file.getName(), fileCustom.getBucket());
//
//                        // 添加已上传文件的 URL
//                        if (ossFileMerge != null && StringUtils.hasText(ossFileMerge.getUrl())) {
//                            uploadedFileUrls.add(ossFileMerge.getUrl());
//                        }
//                    } catch (Exception e) {
//                        log.error("上传文件 {} 失败: {}", file.getName(), e.getMessage());
//                    }
//                } else {
//                    log.warn("{} 不是一个文件，将被跳过", file.getName());
//                }
//            }
//        } else {
//            log.warn("文件夹 {} 为空", fileCustom.getLocalFilePath());
//        }
//
//        return uploadedFileUrls; // 返回所有上传成功文件的 URL 列表
//    }


//    public String localFileMultipartUpload(FileCustomUploadVO fileCustomUploadVO) {
//        OssFile ossFileMerge = null;
//        try {
//            File file = new File(fileCustomUploadVO.getLocalFilePath());
//            if (!file.exists()) {
//                log.error("本地文件不存在");
//            }
//            OssFile ossFile = minioUtils.putChunkObject(fileCustomUploadVO);
//            String originBucketName = ossFile.getBucketName();
//            String objectName = "";
//            if (StringUtils.hasText(fileCustomUploadVO.getPath())) {
//                objectName = fileCustomUploadVO.getPath() + "/" + ossFile.getOriginalFileName();
//            } else {
//                objectName = ossFile.getOriginalFileName();
//            }
//            ossFileMerge = minioUtils.composeObject(originBucketName, fileCustomUploadVO.getBucket(), objectName,fileCustomUploadVO.getHost(),fileCustomUploadVO.getKey(),fileCustomUploadVO.getAppSecret());
//            log.info("桶：{} 中的分片文件，已经在桶：{},文件 {} 合并成功", originBucketName, fileCustomUploadVO.getBucket(), objectName);
//            // 合并成功之后删除对应的临时桶
//            minioUtils.removeBucket(originBucketName, true,fileCustomUploadVO.getHost(),fileCustomUploadVO.getKey(),fileCustomUploadVO.getAppSecret());
//            log.info("删除桶 {} 成功", originBucketName);
//        } catch (Exception e) {
//            log.error("minio-localFileMultipartUpload分段上传错误");
//            e.printStackTrace();
//        }
//        if (ossFileMerge == null || !StringUtils.hasText(ossFileMerge.getUrl())) {
//            return "";
//        }
//        return ossFileMerge.getUrl();
//    }

    public String getSdSignedUrl(String objectKey, Integer expireSeconds, String bucketName,String host,String  key,String appSecret) {
        return minioUtils.getObjectUrl(bucketName, objectKey, host,key,appSecret);
    }
}
