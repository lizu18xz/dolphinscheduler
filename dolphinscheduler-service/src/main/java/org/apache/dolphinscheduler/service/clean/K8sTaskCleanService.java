package org.apache.dolphinscheduler.service.clean;

public interface K8sTaskCleanService {

    void cleanK8sTaskStorageDir(Integer processInstanceId);

}
