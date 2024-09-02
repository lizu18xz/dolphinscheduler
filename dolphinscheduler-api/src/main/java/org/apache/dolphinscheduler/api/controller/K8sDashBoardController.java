package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.dolphinscheduler.api.exceptions.ApiException;
import org.apache.dolphinscheduler.api.service.K8sDashBoardService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import static org.apache.dolphinscheduler.api.enums.Status.K8S_DASHBOARD_NOT_EXIST;

@Tag(name = "K8S_DASHBOARD_TAG")
@RestController
@RequestMapping("/k8s-dashboard")
public class K8sDashBoardController extends BaseController {

    @Autowired
    private K8sDashBoardService k8sDashBoardService;

    /**
     * 获取k8s job地址
     */

    @GetMapping(value = "/pod-monitor-k8s-dashboard")
    @ResponseStatus(HttpStatus.OK)
    @ApiException(K8S_DASHBOARD_NOT_EXIST)
    public Result<String> getK8sDashBoardPodMonitorAddress(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                           @RequestParam(value = "taskInstanceId") int taskInstanceId) {

        return k8sDashBoardService.getK8sJobDashBoardPodMoniotrUrl(taskInstanceId);

    }


}
