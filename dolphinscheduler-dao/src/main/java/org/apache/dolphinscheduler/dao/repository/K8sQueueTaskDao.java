package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.K8sQueueTask;

import java.util.Map;

public interface K8sQueueTaskDao extends IDao<K8sQueueTask> {

    void updateStatus(long taskCode, String status);

    void updateByStatusAndTaskInstanceId(long taskCode, String status, int taskInstanceId);

}
