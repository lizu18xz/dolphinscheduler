package org.apache.dolphinscheduler.api.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.utils.*;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.dolphinscheduler.api.enums.Status.EXTERNAL_ADDRESS_NOT_EXIST;
import static org.apache.dolphinscheduler.common.constants.Constants.EXTERNAL_ADDRESS_LIST;


@Service
@Slf4j
public class MinioSdFileServiceImpl {

    @Autowired
    private SdFileMinioUtils minioUtils;
    @Autowired
    private SdFileObsUtils obsUtils;
    public static final String FETCH_PATH = "/admin-api/system/workRecord/insert";

    public Boolean localFolderMultipartUpload(FileCustomUploadVO fileCustom) {
        List<Map<String, Object>> uploadResponses = new ArrayList<>();
        File folder = new File(fileCustom.getLocalFilePath());

        // 检查文件夹是否存在
        if (!folder.exists() || !folder.isDirectory()) {
            log.info("指定的路径不存在或不是一个文件夹: " + fileCustom.getLocalFilePath());
            return false;
        }


        // 递归遍历文件夹
        if(fileCustom.getOssType().equals("minio")){
            uploadFilesFromFolder(folder, fileCustom, uploadResponses);
            updateMinio(fileCustom, uploadResponses);
        }else {
            uploadObsFilesFromFolder(folder, fileCustom, uploadResponses);
            updateMinio(fileCustom, uploadResponses);
        }

        return true;
    }

    /**
     * obs上传
     * @param folder
     * @param fileCustom
     * @param uploadResponses
     */
    private void uploadObsFilesFromFolder(File folder, FileCustomUploadVO fileCustom, List<Map<String, Object>> uploadResponses) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) { // 确保是文件而不是子文件夹
                    String objectKey = file.getName(); // 使用文件名作为对象的 key
                    String path = file.getAbsolutePath();
                    if (fileCustom.getType() == 0) {
                        //TODO 临时使用
                        fileCustom.setBucketName("defect-data");
                        fileCustom.setHost("https://obs.cn-north-11.myhuaweicloud.com");
                        fileCustom.setKey("3AHTIUS5C9EELB2YKAIV");
                        fileCustom.setAppSecret("kCrRFSwhcIflFssvaMmzKbKdgIIbCeCYNheY5QVy");
                        int lastDotIndex = objectKey.lastIndexOf(".");
                        String prefix = objectKey.substring(0, lastDotIndex);
                        String suffix = objectKey.substring(lastDotIndex);
                        objectKey = prefix + UUID.randomUUID() + suffix;
//                        path = file.getAbsolutePath();//训练的时候使用本地目录
                    } else {
//                        objectKey = fileCustom.getPath() + objectKey;
//                        path = fileCustom.getPath();//数据集的时候使用数据集的目录
                        objectKey = fileCustom.getPath()+path;
                        objectKey= objectKey.replaceAll("/{2,}", "/");
                    }

                    // 上传文件
                    Boolean uploadSuccess = obsUtils.localFileMultipartUpload(fileCustom.getBucketName(), path, objectKey, fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());
                    log.info("uploadSuccess" + uploadSuccess);
                    log.info(fileCustom.getBucketName() + ":path=" + path + ":objectKey=" + objectKey + ":ileCustom.key()=" + fileCustom.getKey() + ":ileCustom.getHost()=" + fileCustom.getAppSecret());
                    if (uploadSuccess) {
                        // 构建文件的 URL
                        String fileUrl = obsUtils.getSignedUrl(fileCustom.getBucketName(), objectKey, fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());
                        Map<String, Object> map = new HashMap<>(16);
                        map.put("key", path + "/" + objectKey);
                        map.put("size", file.length());
                        map.put("objectKey", objectKey);
                        map.put("url", fileUrl);
                        log.info("fileUrl" + fileUrl);
                        uploadResponses.add(map);
                    } else {
                        log.error("文件上传失败: {}", file.getName());
                    }
                } else if (file.isDirectory()) { // 如果是目录，递归处理
                    uploadFilesFromFolder(file, fileCustom, uploadResponses);
                }
            }
        } else {
            throw new IllegalArgumentException("文件夹 " + folder.getAbsolutePath() + " 为空");
        }
    }

    private void uploadFilesFromFolder(File folder, FileCustomUploadVO fileCustom, List<Map<String, Object>> uploadResponses) {
        File[] files = folder.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) { // 确保是文件而不是子文件夹
                    String objectKey = file.getName(); // 使用文件名作为对象的 key
                    String path = file.getAbsolutePath();
                    if (fileCustom.getType() == 0) {
                        //TODO 临时使用
                        fileCustom.setBucketName("defect-prod");
                        fileCustom.setHost("http://10.78.5.103:9991");
                        fileCustom.setKey("V0M2NPhCfk1exMSxAbnI");
                        fileCustom.setAppSecret("5Ups2aOwxJQ4oaoVR42QwM1nLHS2GnNIBcSp3XpX");
                        int lastDotIndex = objectKey.lastIndexOf(".");
                        String prefix = objectKey.substring(0, lastDotIndex);
                        String suffix = objectKey.substring(lastDotIndex);
                        objectKey = prefix + UUID.randomUUID() + suffix;
//                        path = file.getAbsolutePath();//训练的时候使用本地目录
                    } else {
//                        objectKey = fileCustom.getPath() + objectKey;
//                        path = fileCustom.getPath();//数据集的时候使用数据集的目录
                        objectKey = fileCustom.getPath()+path;
                        objectKey= objectKey.replaceAll("/{2,}", "/");
                    }
                    // 上传文件
                    Boolean uploadSuccess = minioUtils.uploadObject(fileCustom.getBucketName(), path, objectKey, fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());
                    log.info("uploadSuccess" + uploadSuccess);
                    log.info(fileCustom.getBucketName() + ":path=" + path + ":objectKey=" + objectKey + ":ileCustom.key()=" + fileCustom.getKey() + ":ileCustom.getHost()=" + fileCustom.getAppSecret());
                    if (uploadSuccess) {
                        // 构建文件的 URL
                        String fileUrl = minioUtils.getObjectUrl(fileCustom.getBucketName(), objectKey, fileCustom.getHost(), fileCustom.getKey(), fileCustom.getAppSecret());
                        Map<String, Object> map = new HashMap<>(16);
                        map.put("key", path + "/" + objectKey);
                        map.put("size", file.length());
                        map.put("objectKey", objectKey);
                        map.put("url", fileUrl);
                        log.info("fileUrl" + fileUrl);
                        uploadResponses.add(map);
                    } else {
                        log.error("文件上传失败: {}", file.getName());
                    }
                } else if (file.isDirectory()) { // 如果是目录，递归处理
                    uploadFilesFromFolder(file, fileCustom, uploadResponses);
                }
            }
        } else {
            throw new IllegalArgumentException("文件夹 " + folder.getAbsolutePath() + " 为空");
        }
    }

    public Boolean updateMinio(FileCustomUploadVO fileCustom, List<Map<String, Object>> uploadResponses) {
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        String url = address + FETCH_PATH;
        WordVO wordVO = new WordVO();
        //处理训练平类型
        if (fileCustom.getType() == 0) {
            List<ModelVO> modelList = new ArrayList<>();
            for (Map<String, Object> map : uploadResponses) {
                ModelVO modelVO = new ModelVO();
                modelVO.setModelDesc("");
                modelVO.setModelName((String) map.get("objectKey"));
                modelVO.setModelVersion("1.0");
                modelVO.setProjectName(fileCustom.getProjectName());
                modelVO.setModelFile((String) map.get("url"));
                modelVO.setModelId(fileCustom.getModelId());
                modelVO.setWorkFlowId(fileCustom.getWorkFlowId());
                modelVO.setProjectCode(fileCustom.getProjectName());
                modelList.add(modelVO);
            }
            wordVO.setTenantCode(fileCustom.getTenantCode());
            wordVO.setUserName(fileCustom.getUserName());
            wordVO.setType("0");
            wordVO.setModelList(modelList);
            wordVO.setProjectCode(fileCustom.getProjectName());
        } else {//处理数据处理类型
            if (fileCustom.getDataType() == 0) {//处理数据集数据
                TpDatasetVO tpDatasetVO = new TpDatasetVO();
                tpDatasetVO.setTpDatasetId(fileCustom.getDataId());
                tpDatasetVO.setWorkFlowId(fileCustom.getWorkFlowId());
                tpDatasetVO.setSourceId(fileCustom.getSourceId());
                tpDatasetVO.setDataType(fileCustom.getDataType());
                List<Map<String, Object>> relativePathList = new ArrayList<>();
                for (Map<String, Object> map : uploadResponses) {
                    Map<String, Object> dataMap = new HashMap<>(16);
                    dataMap.put("objectKey", map.get("objectKey"));
                    dataMap.put("size", map.get("size"));
                    relativePathList.add(dataMap);
                }
                tpDatasetVO.setRelativePathList(relativePathList);
                tpDatasetVO.setProjectCode(fileCustom.getProjectName());
                wordVO.setTenantCode(fileCustom.getTenantCode());
                wordVO.setUserName(fileCustom.getUserName());
                wordVO.setType("1");
                wordVO.setTpDatasetVO(tpDatasetVO);
                wordVO.setProjectCode(fileCustom.getProjectName());
            } else {//处理切片/数据类型
                List<Map<String, Object>> relativePathList = new ArrayList<>();
                TpDatasetVO tpDatasetVO = new TpDatasetVO();
                tpDatasetVO.setTpDatasetId(fileCustom.getDataId());
                tpDatasetVO.setWorkFlowId(fileCustom.getWorkFlowId());
                tpDatasetVO.setSourceId(fileCustom.getSourceId());
                tpDatasetVO.setDataType(fileCustom.getDataType());
                tpDatasetVO.setProjectCode(fileCustom.getProjectName());
                for (Map<String, Object> map : uploadResponses) {
                    Map<String, Object> dataMap = new HashMap<>(16);
                    dataMap.put("objectKey", map.get("objectKey"));
                    dataMap.put("size", map.get("size"));
                    dataMap.put("url", map.get("url"));
                    relativePathList.add(dataMap);
                }
                wordVO.setType("1");
                wordVO.setTenantCode(fileCustom.getTenantCode());
                wordVO.setUserName(fileCustom.getUserName());
                tpDatasetVO.setRelativePathList(relativePathList);
                wordVO.setTpDatasetVO(tpDatasetVO);
                wordVO.setProjectCode(fileCustom.getProjectName());
            }
        }
        String msgToJson = JSONUtils.toJsonString(wordVO);
        log.info("minio msgToJson:{}", msgToJson);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(url, msgToJson);
        CloseableHttpClient httpClient;

        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            log.info("minio response:{}", response);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("update minio error, return http status code: {} ", statusCode);
                return false;
            } else {
                log.info("工作流结束业务文件存储成功并更新业务库 code: {} ", statusCode);
                return true;
            }
        } catch (Exception e) {
            log.error("update minio error{}", e);
            return null;
        } finally {
            try {
                response.close();
                httpClient.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String getSdSignedUrl(String objectKey, Integer expireSeconds, String bucketName, String host, String key, String appSecret) {
        return minioUtils.getObjectUrl(bucketName, objectKey, host, key, appSecret);
    }
}
