package org.apache.dolphinscheduler.api.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.dto.external.*;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ExternalSysService;
import org.apache.dolphinscheduler.api.utils.HttpRequestUtil;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.api.enums.Status.*;
import static org.apache.dolphinscheduler.common.constants.Constants.EXTERNAL_ADDRESS_LIST;
import static org.apache.dolphinscheduler.common.constants.Constants.K8S_VOLUME;

@Service
@Lazy
@Slf4j
public class ExternalSysServiceImpl implements ExternalSysService {

    public static final String PROJECT_NAME = "projectName";
    public static final String HARBOR_IMAGE_PATH = "/admin-api/pipeline/harbor/getImageList";
    public static final String FETCH_PATH = "/admin-api/system/base-tp-dataset-detail/folderTreeAll";
    public static final String MODEL_PATH = "/admin-api/system/model/getModelList";
    public static final String STORAGE_PAGE = "/admin-api/system/storage/page";
    public static final String TREE1 = "/admin-api/system/baseOriginalData/haitun/folderOriginalTreeAll";
    public static final String TREE2 = "/admin-api/system/baseOriginalData/haitun/folderProblemTreeAll";
    public static final String TREE3 = "/admin-api/system/sdFileDetail/haitun/sdFileDetailFolderTreeAll";
    public static final String FILE1 = "/admin-api/system/baseOriginalData/haitun/haitunOriginalFileByParentId";
    public static final String FILE2 = "/admin-api/system/baseOriginalData/haitun/haitunProblemFileByParentId";
    public static final String FILE3 = "/admin-api/system/sdFileDetail/haitun/haitunSdFileByParentId";

    @Autowired
    private ProjectMapper projectMapper;

    @Override
    public List<ImageResponse> imageList(ImageRequest imageRequest) {
        if (StringUtils.isEmpty(imageRequest.getProjectName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, PROJECT_NAME);
        }
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        //获取项目因为名称，对应仓库的名称
        Project project = projectMapper.queryByName(imageRequest.getProjectName());
        imageRequest.setProjectName(project.getProjectEnName() == null ? project.getName() : project.getProjectEnName());

        //发送http请求
        String msgToJson = JSONUtils.toJsonString(imageRequest);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(address + HARBOR_IMAGE_PATH, msgToJson);
        CloseableHttpClient httpClient;

        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get image list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            ObjectNode result = JSONUtils.parseObject(resp);
            log.info("获取镜像列表 resp:{}", resp);
            if (result.get("data") == null) {
                log.info("获取镜像列表失败");
                return new ArrayList<>();
            }
            String data = result.get("data").toString();
            List<ImageResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<ImageResponse>>() {
            });
            return responses;
        } catch (Exception e) {
            log.error("get image error:{},e:{}", msgToJson, e);
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

    @Override
    public List<WrapFetchVolumeResponse> fetchVolumeList(FetchVolumeRequest request) {

        //获取项目因为名称，对应仓库的名称
        Project project = projectMapper.queryByName(request.getProjectName());

        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        String url = address + FETCH_PATH;
        String msgToJson = JSONUtils.toJsonString(request);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(url, msgToJson);
        CloseableHttpClient httpClient;

        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get volume list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            log.info("fetchVolumeList resp:{}", resp);
            ObjectNode result = JSONUtils.parseObject(resp);
            if (result.get("data") == null) {
                log.info("获取fetch存储列表失败");
                return new ArrayList<>();
            }
            String data = result.get("data").toString();
            List<DataSetResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<DataSetResponse>>() {
            });
            long projectCode = project.getCode();
            String k8sVolume = PropertyUtils.getString(K8S_VOLUME);
            String fetchPath = k8sVolume + "/" + projectCode + "/fetch/";
            List<WrapFetchVolumeResponse> inputVolumeResponseList = new ArrayList<>();

            parseParent(responses, inputVolumeResponseList, fetchPath, projectCode);

            return inputVolumeResponseList;
        } catch (Exception e) {
            log.error("get volume error{}", e);
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


    private void parseParent(List<DataSetResponse> responses, List<WrapFetchVolumeResponse> responsesList, String fetchPath, long projectCode) {
        for (DataSetResponse response : responses) {
            WrapFetchVolumeResponse inputVolumeResponse = new WrapFetchVolumeResponse();
            if (response.getType() == null) {
                response.setType("minio");
            }
            if (!response.getType().equals(TaskConstants.VOLUME_LOCAL)) {
                StringBuilder args = new StringBuilder();
                args.append("[").append("\"").append(response.getType()).append("\"").append(",").append("\"")
                        .append(response.getHost()).append("\"").append(",").append("\"").append(response.getAppKey()).append("\"")
                        .append(",").append("\"").append(response.getAppSecret()).append("\"").append(",").append("\"")
                        .append(response.getBucketName()).append("\"").append(",").append("\"")
                        .append(response.getRelativePath()).append("\"").append(",")
                        //容器内部地址写死
                        .append("\"").append("/app/downloads").append("\"").append(",").append("]");
                inputVolumeResponse.setFetchId(response.getId());
                inputVolumeResponse.setFetchName(response.getName());
                //拉取数据的参数
                inputVolumeResponse.setFetchDataVolumeArgs(args.toString());
                //宿主机路径,拉取镜像存储的宿主机路径,最外层,具体目录需要在内部自己修改
                inputVolumeResponse.setFetchDataVolume(fetchPath);
                inputVolumeResponse.setFetchType(response.getType());
                responsesList.add(inputVolumeResponse);
            }
            if (!CollectionUtils.isEmpty(response.getChildren())) {
                parse(response.getChildren(), inputVolumeResponse, fetchPath, projectCode);
            }
        }
    }

    private void parse(List<DataSetResponse> responses, WrapFetchVolumeResponse inputVolumeResponseParent, String fetchPath, long projectCode) {
        List<WrapFetchVolumeResponse> inputVolumeResponseList = new ArrayList<>();
        for (DataSetResponse response : responses) {
            WrapFetchVolumeResponse inputVolumeResponse = new WrapFetchVolumeResponse();
            if (response.getType() == null) {
                response.setType("minio");
            }
            if (!response.getType().equals(TaskConstants.VOLUME_LOCAL)) {
                StringBuilder args = new StringBuilder();
                args.append("[").append("\"").append(response.getType()).append("\"").append(",").append("\"").append(response.getHost()).append("\"").append(",").append("\"").append(response.getAppKey()).append("\"").append(",").append("\"").append(response.getAppSecret()).append("\"").append(",").append("\"").append(response.getBucketName()).append("\"").append(",").append("\"").append(response.getRelativePath()).append("\"").append(",")
                        //容器内部地址写死
                        .append("\"").append("/app/downloads").append("\"").append(",").append("]");
                inputVolumeResponse.setFetchId(response.getId());
                inputVolumeResponse.setFetchName(response.getName());
                //拉取数据的参数
                inputVolumeResponse.setFetchDataVolumeArgs(args.toString());
                //宿主机路径,拉取镜像存储的宿主机路径,最外层,具体目录需要在内部自己修改
                inputVolumeResponse.setFetchDataVolume(fetchPath);
                inputVolumeResponse.setFetchType(response.getType());
                inputVolumeResponseList.add(inputVolumeResponse);
            }
            if (!CollectionUtils.isEmpty(response.getChildren())) {
                parse(response.getChildren(), inputVolumeResponse, fetchPath, projectCode);
            }
        }
        if (!CollectionUtils.isEmpty(inputVolumeResponseList)) {
            inputVolumeResponseParent.setChildren(inputVolumeResponseList);
        }
    }


    @Override
    public List<StorageResponse> storagePage(StorageRequest request) {
        if (StringUtils.isEmpty(request.getProjectName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, PROJECT_NAME);
        }
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        //发送http请求
        String msgToJson = JSONUtils.toJsonString(request);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(address + STORAGE_PAGE, msgToJson);
        CloseableHttpClient httpClient;
        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get image list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            log.info("store page resp :{}", resp.toString());
            ObjectNode result = JSONUtils.parseObject(resp);
            String data = result.get("data").get("list").toString();
            List<StorageResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<StorageResponse>>() {
            });
            return responses;
        } catch (Exception e) {
            log.error("get image error:{},e:{}", msgToJson, e);
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

    @Override
    public List<OutPutVolumeResponse> getVolumeOutput(StorageRequest request, String type) {
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        String url = address + FETCH_PATH;
        if (type == null) {
            type = "0";
        }
        if (StringUtils.isEmpty(type)) {
            type = "0";
        }
        if (type.equals("1")) {
            url = address + TREE2;
        }
        String msgToJson = JSONUtils.toJsonString(request);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(url, msgToJson);
        CloseableHttpClient httpClient;

        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get volume list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            log.info("outputFetchVolumeList resp:{}", resp);
            ObjectNode result = JSONUtils.parseObject(resp);
            if (result.get("data") == null) {
                log.info("获取output fetch存储列表失败");
                return new ArrayList<>();
            }
            String data = result.get("data").toString();
            List<DataSetResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<DataSetResponse>>() {
            });

            List<OutPutVolumeResponse> outPutVolumeResponseList = new ArrayList<>();
            parseOutPutParent(responses, outPutVolumeResponseList);

            return outPutVolumeResponseList;
        } catch (Exception e) {
            log.error("get output volume error{}", e);
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

    @Override
    public List<ModelResponse> getModelList(StorageRequest request) {
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        //获取项目因为名称，对应仓库的名称
        Project project = projectMapper.queryByName(request.getProjectName());
        request.setProjectName(project.getProjectEnName());
        String url = address + MODEL_PATH + "?projectName=" + project.getProjectEnName();
        HttpGet get = new HttpGet(url);
        CloseableHttpClient httpClient;
        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(get);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get model list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            log.info("model resp:{}", resp);
            ObjectNode result = JSONUtils.parseObject(resp);
            if (result.get("data") == null) {
                log.info("获取model存储列表失败");
                return new ArrayList<>();
            }
            String data = result.get("data").toString();
            List<ModelResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<ModelResponse>>() {
            });
            responses = responses.stream().peek(x -> x.setModelId(x.getId())).collect(Collectors.toList());
            return responses;
        } catch (Exception e) {
            log.error("get model list error{}", e);
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

    @Override
    public List<TreeResponse> getDataSetTree(String type) {
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        String url = address;
        if (type.equals("0")) {
            url = url + TREE1;
        } else if (type.equals("1")) {
            url = url + TREE2;
        } else {
            url = url + TREE3;
        }
        Map<String, Object> map = new HashMap<>();
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(url, JSONUtils.toJsonString(map));
        CloseableHttpClient httpClient;
        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get tree list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            log.info("model resp:{}", resp);
            ObjectNode result = JSONUtils.parseObject(resp);
            if (result.get("data") == null) {
                log.info("获取model存储列表失败");
                return new ArrayList<>();
            }
            String data = result.get("data").toString();
            List<TreeResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<TreeResponse>>() {
            });

            return responses;
        } catch (Exception e) {
            log.error("get tree list error{}", e);
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

    @Override
    public List<WrapFetchVolumeResponse> getDataSetFileList(String type, String projectName, String dirId) {
        String address = PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_ADDRESS_NOT_EXIST.getMsg());
        }
        String url = address;
        if (type.equals("0")) {
            url = url + FILE1;
        } else if (type.equals("1")) {
            url = url + FILE2;
        } else {
            url = url + FILE3;
        }

        Map<String, Object> map = new HashMap<>();
        map.put("id", dirId);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(url, JSONUtils.toJsonString(map));
        CloseableHttpClient httpClient;
        httpClient = HttpRequestUtil.getHttpClient();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(httpPost);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                log.error("get file list error, return http status code: {} ", statusCode);
            }
            String resp;
            HttpEntity entity = response.getEntity();
            resp = EntityUtils.toString(entity, "utf-8");
            log.info("get dataSetFileList resp:{}", resp);
            ObjectNode result = JSONUtils.parseObject(resp);
            if (result.get("data") == null) {
                log.info("获取fetch存储列表失败");
                return new ArrayList<>();
            }
            String data = result.get("data").toString();
            List<FileResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<FileResponse>>() {
            });

            Project project = projectMapper.queryByName(projectName);
            long projectCode = project.getCode();
            String k8sVolume = PropertyUtils.getString(K8S_VOLUME);
            String fetchPath = k8sVolume + "/" + projectCode + "/fetch/";
            List<WrapFetchVolumeResponse> fetchVolumeResponses = new ArrayList<>();
            for (FileResponse fileResponse : responses) {
                if (fileResponse.getType() == null) {
                    fileResponse.setType("minio");
                }
                WrapFetchVolumeResponse inputVolumeResponse = new WrapFetchVolumeResponse();
                StringBuilder args = new StringBuilder();
                args.append("[").append("\"").append(fileResponse.getType()).append("\"").append(",").append("\"").append(fileResponse.getHost()).append("\"").append(",").append("\"").append(fileResponse.getAppKey()).append("\"").append(",").append("\"").append(fileResponse.getAppSecret()).append("\"").append(",").append("\"").append(fileResponse.getBucketName()).append("\"").append(",").append("\"").append(fileResponse.getRelativePath()).append("\"").append(",")
                        //容器内部地址写死
                        .append("\"").append("/app/downloads").append("\"").append(",").append("]");
                inputVolumeResponse.setFetchId(fileResponse.getId());
                inputVolumeResponse.setFetchName(fileResponse.getFileName());
                //拉取数据的参数
                inputVolumeResponse.setFetchDataVolumeArgs(args.toString());
                //宿主机路径,拉取镜像存储的宿主机路径,最外层,具体目录需要在内部自己修改
                inputVolumeResponse.setFetchDataVolume(fetchPath);
                inputVolumeResponse.setFetchType(fileResponse.getType());
                inputVolumeResponse.setDirId(dirId);
                fetchVolumeResponses.add(inputVolumeResponse);
            }
            return fetchVolumeResponses;
        } catch (Exception e) {
            log.error("get data set file info error{}", e);
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

    private void parseOutPutParent(List<DataSetResponse> responses, List<OutPutVolumeResponse> outPutVolumeResponseList) {
        if (CollectionUtils.isEmpty(responses)) {
            return;
        }
        for (DataSetResponse response : responses) {
            OutPutVolumeResponse outPutVolumeResponse = new OutPutVolumeResponse();
            if (response.getType() == null) {
                response.setType("minio");
            }
            if (!response.getType().equals(TaskConstants.VOLUME_LOCAL)) {
                outPutVolumeResponse.setOutputVolumeId(response.getId());
                outPutVolumeResponse.setOutputVolumeName(response.getName());
                outPutVolumeResponse.setOutputVolumeNameInfo(JSONUtils.toJsonString(response));
                outPutVolumeResponseList.add(outPutVolumeResponse);
            }
            if (!CollectionUtils.isEmpty(response.getChildren())) {
                outPutParse(response.getChildren(), outPutVolumeResponse);
            }
        }
    }

    private void outPutParse(List<DataSetResponse> responses, OutPutVolumeResponse parentOutPutVolumeResponse) {
        List<OutPutVolumeResponse> outPutVolumeResponseList = new ArrayList<>();
        for (DataSetResponse response : responses) {
            OutPutVolumeResponse outPutVolumeResponse = new OutPutVolumeResponse();
            if (response.getType() == null) {
                response.setType("minio");
            }
            if (!response.getType().equals(TaskConstants.VOLUME_LOCAL)) {
                outPutVolumeResponse.setOutputVolumeId(response.getId());
                outPutVolumeResponse.setOutputVolumeName(response.getName());
                outPutVolumeResponse.setOutputVolumeNameInfo(JSONUtils.toJsonString(response));
                outPutVolumeResponseList.add(outPutVolumeResponse);
            }
            if (!CollectionUtils.isEmpty(response.getChildren())) {
                outPutParse(response.getChildren(), outPutVolumeResponse);
            }
        }
        if (!CollectionUtils.isEmpty(outPutVolumeResponseList)) {
            parentOutPutVolumeResponse.setChildren(outPutVolumeResponseList);
        }
    }


}
