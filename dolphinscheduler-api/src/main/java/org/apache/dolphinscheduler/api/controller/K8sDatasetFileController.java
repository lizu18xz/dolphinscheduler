package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sDatasetFileRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sDatasetFileResponse;
import org.apache.dolphinscheduler.api.service.K8sDatasetFileService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "K8S_DATASET")
@RestController
@Slf4j
@RequestMapping("/k8s-dataset")
public class K8sDatasetFileController extends BaseController {

    @Autowired
    private K8sDatasetFileService k8sDatasetFileService;


    @GetMapping(value = "/page")
    @ResponseStatus(HttpStatus.OK)
    public Result<PageInfo<K8sDatasetFileResponse>> page(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                         @RequestParam(value = "taskInstanceId", required = false) String taskInstanceId,
                                                         @RequestParam("pageSize") Integer pageSize,
                                                         @RequestParam("pageNo") Integer pageNo) {

        Result result = checkPageParams(pageNo, pageSize);
        if (!result.checkResult()) {
            log.warn("Pagination parameters check failed, pageNo:{}, pageSize:{}", pageNo, pageSize);
            return result;
        }

        return k8sDatasetFileService.queryListPaging(loginUser, pageSize, pageNo, taskInstanceId);
    }


    /**
     * 创建队列
     */
    @PostMapping(value = "/create")
    @ResponseStatus(HttpStatus.OK)
    public Result create(@RequestBody K8sDatasetFileRequest request) {
        log.info("create queue:{}", JSONUtils.toPrettyJsonString(request));
        //保存数据库
        return k8sDatasetFileService.create(request);
    }


    /**
     * 创建队列
     */
    @PostMapping(value = "/createBatch")
    @ResponseStatus(HttpStatus.OK)
    public Result batchCreate(@RequestBody List<K8sDatasetFileRequest> requests) {
        log.info("create queue:{}", JSONUtils.toPrettyJsonString(requests));
        //保存数据库
        return k8sDatasetFileService.batchCreate(requests);
    }

}
