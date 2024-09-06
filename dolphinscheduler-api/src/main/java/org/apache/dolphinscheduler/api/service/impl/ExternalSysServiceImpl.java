package org.apache.dolphinscheduler.api.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.dto.external.ImageRequest;
import org.apache.dolphinscheduler.api.dto.external.ImageResponse;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.ExternalSysService;
import org.apache.dolphinscheduler.api.utils.HttpRequestUtil;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

import static org.apache.dolphinscheduler.api.enums.Status.*;
import static org.apache.dolphinscheduler.common.constants.Constants.EXTERNAL_IMAGE_LIST;

@Service
@Lazy
@Slf4j
public class ExternalSysServiceImpl implements ExternalSysService {

    public static final String PROJECT_NAME = "projectName";

    @Override
    public List<ImageResponse> imageList(ImageRequest imageRequest) {
        if (StringUtils.isEmpty(imageRequest.getProjectName())) {
            throw new ServiceException(Status.REQUEST_PARAMS_NOT_VALID_ERROR, PROJECT_NAME);
        }
        String address =
                PropertyUtils.getString(EXTERNAL_IMAGE_LIST);
        if (StringUtils.isEmpty(address)) {
            throw new IllegalArgumentException(EXTERNAL_IMAGE_LIST_NOT_EXIST.getMsg());
        }
        //发送http请求
        String msgToJson = JSONUtils.toJsonString(imageRequest);
        HttpPost httpPost = HttpRequestUtil.constructHttpPost(address, msgToJson);
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
}
