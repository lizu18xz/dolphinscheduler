package org.apache.dolphinscheduler.dao.repository;

import org.apache.dolphinscheduler.dao.entity.K8sQueueTask;

public interface K8sQueueTaskDao extends IDao<K8sQueueTask> {

    void updateStatus(long taskCode, String status);

}
