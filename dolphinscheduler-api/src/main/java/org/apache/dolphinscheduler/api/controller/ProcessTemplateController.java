package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.dto.processTemplate.ProcessTemplateRequest;
import org.apache.dolphinscheduler.api.dto.processTemplate.ProcessTemplateResponse;
import org.apache.dolphinscheduler.api.service.ProcessTemplateService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@Tag(name = "K8S_DATASET")
@RestController
@Slf4j
@RequestMapping("/process-template")
public class ProcessTemplateController extends BaseController {

    @Autowired
    private ProcessTemplateService processTemplateService;


    @PostMapping(value = "/create")
    @ResponseStatus(HttpStatus.OK)
    public Result create(@RequestBody ProcessTemplateRequest request) {
        log.info("create template:{}", JSONUtils.toPrettyJsonString(request));
        //保存数据库
        return processTemplateService.create(request);
    }


    @GetMapping(value = "/page")
    @ResponseStatus(HttpStatus.OK)
    public Result<PageInfo<ProcessTemplateResponse>> page(@Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
                                                          @RequestParam(value = "keyword", required = false) String keyword,
                                                          @RequestParam("pageSize") Integer pageSize,
                                                          @RequestParam("pageNo") Integer pageNo) {

        Result result = checkPageParams(pageNo, pageSize);
        if (!result.checkResult()) {
            log.warn("Pagination parameters check failed, pageNo:{}, pageSize:{}", pageNo, pageSize);
            return result;
        }

        return processTemplateService.queryListPaging(loginUser, pageSize, pageNo, keyword);
    }

}
