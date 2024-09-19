package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueTaskResponse;
import org.apache.dolphinscheduler.api.service.K8sQueueTaskService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "K8S_QUEUE_TASK")
@RestController
@Slf4j
@RequestMapping("/k8s-queue-task")
public class K8sQueueTaskController extends BaseController {

    @Autowired
    private K8sQueueTaskService k8sQueueTaskService;

    @GetMapping(value = "/page")
    @ResponseStatus(HttpStatus.OK)
    public Result<PageInfo<K8sQueueTaskResponse>> page(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                       @RequestParam(value = "searchVal", required = false) String searchVal,
                                                       @RequestParam("pageSize") Integer pageSize,
                                                       @RequestParam("pageNo") Integer pageNo) {

        Result result = checkPageParams(pageNo, pageSize);
        if (!result.checkResult()) {
            log.warn("Pagination parameters check failed, pageNo:{}, pageSize:{}", pageNo, pageSize);
            return result;
        }

        return k8sQueueTaskService.queryK8sQueueTaskListPaging(loginUser, pageSize, pageNo, searchVal);
    }




}
