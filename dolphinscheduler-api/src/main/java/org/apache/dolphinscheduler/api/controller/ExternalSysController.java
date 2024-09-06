package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.dolphinscheduler.api.dto.external.ImageRequest;
import org.apache.dolphinscheduler.api.dto.external.ImageResponse;
import org.apache.dolphinscheduler.api.service.ExternalSysService;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@Tag(name = "EXTERNAL_SYS")
@RestController
@RequestMapping("/external-sys")
public class ExternalSysController {

    @Autowired
    private ExternalSysService externalSysService;


    /**
     * http://10.78.5.104:48080/admin-api/harbor/user/getImageList
     * 获取当前项目下面的镜像
     */
    @GetMapping(value = "/image-list")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<ImageResponse>> getK8sDashBoardPodMonitorAddress(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName) {
        ImageRequest imageRequest = new ImageRequest();
        imageRequest.setProjectName(projectName);
        return Result.success(externalSysService.imageList(imageRequest));
    }


}
