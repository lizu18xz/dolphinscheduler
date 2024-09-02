package org.apache.dolphinscheduler.api.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.K8sDashBoardService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.dao.repository.TaskDefinitionDao;
import org.apache.dolphinscheduler.dao.repository.TaskInstanceDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Map;

import static org.apache.dolphinscheduler.api.enums.Status.K8S_DASHBOARD_NOT_EXIST;
import static org.apache.dolphinscheduler.common.constants.Constants.NAMESPACE;

@Service
@Slf4j
public class K8SDashBoardServiceImpl extends BaseServiceImpl implements K8sDashBoardService {

    @Autowired
    private TaskInstanceDao taskInstanceDao;

    @Autowired
    private TaskDefinitionDao taskDefinitionDao;


    @Override
    public Result<String> getK8sJobDashBoardPodMoniotrUrl(int taskInstanceId) {
        String dashBoardAddress =
                PropertyUtils.getString(Constants.K8S_DASHBOARD_ADDRESS);
        if (StringUtils.isEmpty(dashBoardAddress)) {
            return Result.error(K8S_DASHBOARD_NOT_EXIST);
        }

        TaskInstance taskInstance = taskInstanceDao.queryById(taskInstanceId);
        if (taskInstance == null) {
            log.error("Task instance does not exist, taskInstanceId:{}.", taskInstanceId);
            return Result.error(Status.TASK_INSTANCE_NOT_FOUND);
        }
        //获取任务名称
        long taskCode = taskInstance.getTaskCode();
        int taskDefinitionVersion = taskInstance.getTaskDefinitionVersion();
        TaskDefinition taskDefinition = taskDefinitionDao.findTaskDefinition(taskCode, taskDefinitionVersion);
        String nodeString = JSONUtils.getNodeString(taskDefinition.getTaskParams(), NAMESPACE);
        Map<String, String> map = JSONUtils.toMap(nodeString);
        String nameSpaceName = map.get("name");

        String taskName = taskInstance.getName().toLowerCase(Locale.ROOT);
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);
        dashBoardAddress = dashBoardAddress + "pod?namespace=" + nameSpaceName + "&q=" + k8sJobName;
        log.info("get k8s dashboard pod address:{}", dashBoardAddress);

        return Result.success(dashBoardAddress);
    }
}
