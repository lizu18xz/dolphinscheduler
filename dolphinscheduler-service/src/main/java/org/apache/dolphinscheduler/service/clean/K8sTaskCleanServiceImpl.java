package org.apache.dolphinscheduler.service.clean;

import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.apache.dolphinscheduler.common.constants.Constants.K8S_VOLUME;

@Component
@Slf4j
public class K8sTaskCleanServiceImpl implements K8sTaskCleanService {

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Override
    public void cleanK8sTaskStorageDir(Integer processInstanceId) {

        List<TaskInstance> taskInstances = taskInstanceDao.queryByWorkflowInstanceId(processInstanceId);
        log.info("clean taskInstances size :{}", taskInstances.size());
        for (TaskInstance taskInstance : taskInstances) {
            String taskType = taskInstance.getTaskType();
            if (taskType.equals("K8S") || taskType.equals("PYTORCH_K8S") || taskType.equals("DATA_SET_K8S")) {
                log.info("start del k8s task dir");

                String taskOutPutPath = PropertyUtils.getString(K8S_VOLUME) + "/" + taskInstance.getProjectCode()
                        + "/output/" + taskInstance.getId();
                String taskFetchPath = PropertyUtils.getString(K8S_VOLUME) + "/" + taskInstance.getProjectCode()
                        + "/fetch/" + taskInstance.getId();

                log.info("del output:{},fetch:{}", taskOutPutPath, taskFetchPath);

            }
        }
    }


}
