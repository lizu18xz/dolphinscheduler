package org.apache.dolphinscheduler.api.controller;

import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.service.MinioFileService;
import org.apache.dolphinscheduler.api.utils.FileCustomUploadVO;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

@RestController
@RequestMapping("/fileCustom")
@Slf4j
public class FileMinioController {

    @Resource
    private MinioFileService sdFileService;

    @Operation(summary = "文件上传")
    @RequestMapping(value = "/sdUpload")
    public Boolean sdFileCustomUpload(@RequestBody FileCustomUploadVO fileCustomUploadVO) {
        log.info("sdFileCustomUpload:{}", JSONUtils.toJsonString(fileCustomUploadVO));
        return sdFileService.localFileMultipartUpload(fileCustomUploadVO);
    }


    @Operation(summary = "获取文件的临时访问路径")
    @GetMapping("/getSdSignedUrl")
    public String getSdSignedUrl(@RequestParam(required = true, name = "fileName") String fileName
            , @RequestParam(required = false, name = "expireSeconds") Integer expireSeconds
            , @RequestParam(required = false, name = "bucketName") String bucketName, String host, String key, String appSecret) {
        return sdFileService.getSdSignedUrl(fileName, expireSeconds, bucketName, host, key, appSecret);
    }


}
