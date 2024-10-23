package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.dolphinscheduler.api.dto.external.*;
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


    /**
     * 获取挂载配置
     */
    @GetMapping(value = "/volume-list")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<WrapFetchVolumeResponse>> getFetchVolumeList(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName) {
        FetchVolumeRequest request = new FetchVolumeRequest();
        request.setProjectName(projectName);
        return Result.success(externalSysService.fetchVolumeList(request));
    }


    /**
     * 获取挂载配置
     */
    @GetMapping(value = "/storage-list")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<StorageResponse>> getStoragePage(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName) {
        StorageRequest request = new StorageRequest();
        request.setProjectName(projectName);
        request.setPageNo("1");
        request.setPageSize("1000");
        return Result.success(externalSysService.storagePage(request));
    }


    @GetMapping(value = "/volume-output-list")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<OutPutVolumeResponse>> getVolumeOutput(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName, @RequestParam(value = "type", required = false) String type) {
        StorageRequest request = new StorageRequest();
        request.setProjectName(projectName);
        request.setPageNo("1");
        request.setPageSize("1000");
        return Result.success(externalSysService.getVolumeOutput(request, type));
    }


    @GetMapping(value = "/model-list")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<ModelResponse>> getModelList(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName) {
        StorageRequest request = new StorageRequest();
        request.setProjectName(projectName);
        request.setPageNo("1");
        request.setPageSize("1000");
        return Result.success(externalSysService.getModelList(request));
    }


    @GetMapping(value = "/dataset-dir")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<TreeResponse>> getDataSetTreeList(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName,
            @RequestParam(value = "type") String type) {

        return Result.success(externalSysService.getDataSetTree(type));
    }


    @GetMapping(value = "/dataset-file")
    @ResponseStatus(HttpStatus.OK)
    public Result<List<WrapFetchVolumeResponse>> getDataSetFileList(
            @Parameter(hidden = true) @RequestAttribute(value = Constants.SESSION_USER) User loginUser,
            @RequestParam(value = "projectName") String projectName,
            @RequestParam(value = "type") String type, @RequestParam(value = "dirId") String dirId) {

        return Result.success(externalSysService.getDataSetFileList(type, projectName, dirId));
    }

}
