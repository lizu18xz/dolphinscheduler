package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueTaskRequest;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.User;

public interface K8sQueueTaskService {


    Result createOrUpdateK8sQueueTask(K8sQueueTaskRequest request);

    Result queryK8sQueueTaskListPaging(User loginUser, Integer pageSize, Integer pageNo, String projectName);


}
