package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.k8squeue.K8sDatasetFileRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sDatasetFileResponse;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

public interface K8sDatasetFileService {

    Result<PageInfo<K8sDatasetFileResponse>> queryListPaging(User loginUser, Integer pageSize, Integer pageNo, String taskInstanceId);

    Result create(K8sDatasetFileRequest request);

    Result batchCreate(List<K8sDatasetFileRequest> requests);
}
