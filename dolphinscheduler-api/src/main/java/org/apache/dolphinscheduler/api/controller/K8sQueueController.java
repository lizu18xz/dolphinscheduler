package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueCalculateResponse;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueResponse;
import org.apache.dolphinscheduler.api.service.K8sQueueService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "K8S_QUEUE")
@RestController
@Slf4j
@RequestMapping("/k8s-queue")
public class K8sQueueController extends BaseController {

    @Autowired
    private K8sQueueService k8sQueueService;

    /**
     * 创建队列
     */
    @PostMapping(value = "/create")
    @ResponseStatus(HttpStatus.OK)
    public Result create(@RequestBody K8sQueueRequest request) {
        log.info("create queue:{}", JSONUtils.toPrettyJsonString(request));
        //保存数据库
        return k8sQueueService.createK8sQueue(request);
    }

    /**
     * 创建队列
     */
    @PutMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Result update(@PathVariable(value = "id") Long id, K8sQueueRequest request) {

        //保存数据库
        return k8sQueueService.updateK8sQueue(id, request);
    }


    /**
     * 创建队列
     */
    @DeleteMapping(value = "/{id}")
    @ResponseStatus(HttpStatus.OK)
    public Result delete(Long id) {

        //保存数据库
        return k8sQueueService.deleteK8sQueue(id);
    }

    @GetMapping(value = "/page")
    @ResponseStatus(HttpStatus.OK)
    public Result<PageInfo<K8sQueueResponse>> page(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                   @RequestParam(value = "projectName", required = false) String projectName,
                                                   @RequestParam("pageSize") Integer pageSize,
                                                   @RequestParam("pageNo") Integer pageNo) {
        Result result = checkPageParams(pageNo, pageSize);
        if (!result.checkResult()) {
            log.warn("Pagination parameters check failed, pageNo:{}, pageSize:{}", pageNo, pageSize);
            return result;
        }
        return k8sQueueService.queryK8sQueueListPaging(loginUser, pageSize, pageNo, projectName);
    }


    @GetMapping(value = "/gpu-type")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<String>> getGpuType() {

        //保存数据库
        return k8sQueueService.getGpuType();
    }


    /**
     * 资源统计
     */
    @GetMapping(value = "/monitor")
    @ResponseStatus(HttpStatus.OK)
    public Result<K8sQueueCalculateResponse> monitorQueueInfo(String projectName, String type) {

        K8sQueueCalculateResponse response = k8sQueueService.monitorQueueInfo(projectName, type);

        return Result.success(response);
    }


}