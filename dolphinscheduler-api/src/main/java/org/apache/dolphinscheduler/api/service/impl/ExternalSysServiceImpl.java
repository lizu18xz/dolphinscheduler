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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    public static final String STORAGE_PAGE = "/admin-api/system/storage/page";

    @Autowired
    private ProjectMapper projectMapper;

    @Override
    public List<ImageResponse> imageList(ImageRequest imageRequest) {
        if (StringUtils.isEmpty(imageRequest.getProjectName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, PROJECT_NAME);
        }
        String address =
                PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
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
        String address =
                PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
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
            List<FetchVolumeResponse> responses = JSONUtils.parseObject(data, new TypeReference<List<FetchVolumeResponse>>() {
            });

            String k8sVolume =
                    PropertyUtils.getString(K8S_VOLUME);
            List<WrapFetchVolumeResponse> inputVolumeResponseList = new ArrayList<>();
            parse(responses, inputVolumeResponseList, k8sVolume);

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


    private void parse(List<FetchVolumeResponse> responses, List<WrapFetchVolumeResponse> responsesList, String k8sVolume) {

        for (FetchVolumeResponse response : responses) {
            WrapFetchVolumeResponse inputVolumeResponse = new WrapFetchVolumeResponse();
            if (response.getType() == null) {
                response.setType("minio");
            }
            if (!response.getType().equals(TaskConstants.VOLUME_LOCAL)) {
                StringBuilder args = new StringBuilder();
                args.append("[").append("\"").append(response.getType()).append("\"").append(",")
                        .append("\"").append(response.getHost()).append("\"").append(",")
                        .append("\"").append(response.getAppKey()).append("\"").append(",")
                        .append("\"").append(response.getAppSecret()).append("\"").append(",")
                        .append("\"").append(response.getBucketName()).append("\"").append(",")
                        .append("\"").append(response.getFilePath()).append("\"").append(",")
                        //容器内部地址写死
                        .append("\"").append("/app/downloads").append("\"").append(",")
                        .append("]");
                inputVolumeResponse.setFetchName(response.getName());
                //拉取数据的参数
                inputVolumeResponse.setFetchDataVolumeArgs(args.toString());
                //宿主机路径,拉取镜像存储的宿主机路径,最外层,具体目录需要在内部自己修改
                inputVolumeResponse.setFetchDataVolume(k8sVolume + "/fetch/");
                inputVolumeResponse.setFetchType(response.getType());
                responsesList.add(inputVolumeResponse);
            }
            if (!CollectionUtils.isEmpty(response.getChildren())) {
                parse(response.getChildren(), responsesList, k8sVolume);
            }
        }

    }


    @Override
    public List<StorageResponse> storagePage(StorageRequest request) {
        if (StringUtils.isEmpty(request.getProjectName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, PROJECT_NAME);
        }
        String address =
                PropertyUtils.getString(EXTERNAL_ADDRESS_LIST);
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


}
