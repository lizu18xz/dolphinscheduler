package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueCalculateResponse;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueResponse;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.K8sQueue;
import org.apache.dolphinscheduler.dao.entity.User;

import java.util.List;

public interface K8sQueueService {

    Result createK8sQueue(K8sQueueRequest request);

    Result updateK8sQueue(Long id, K8sQueueRequest request);

    Result deleteK8sQueue(Long id);

    Result<PageInfo<K8sQueueResponse>> queryK8sQueueListPaging(User loginUser, Integer pageSize, Integer pageNo, String searchVal);

    K8sQueue findByName(String name);

    Result<List<String>> getGpuType();

    K8sQueueCalculateResponse monitorQueueInfo(String projectName, String type);
}
