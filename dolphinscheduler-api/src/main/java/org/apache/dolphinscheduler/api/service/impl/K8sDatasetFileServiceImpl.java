package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sDatasetFileRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sDatasetFileResponse;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.K8sDatasetFileService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.K8sDatasetFile;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.K8sDatasetFileMapper;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Lazy
@Slf4j
public class K8sDatasetFileServiceImpl extends BaseServiceImpl implements K8sDatasetFileService {

    @Autowired
    private K8sDatasetFileMapper k8sDatasetFileMapper;

    @Override
    public Result<PageInfo<K8sDatasetFileResponse>> queryListPaging(User loginUser, Integer pageSize, Integer pageNo, String processInstanceId) {
        Result<PageInfo<K8sDatasetFileResponse>> result = new Result();
        PageInfo<K8sDatasetFileResponse> pageInfo = new PageInfo<>(pageNo, pageSize);
        Page<K8sDatasetFile> page = new Page<>(pageNo, pageSize);
        QueryWrapper<K8sDatasetFile> wrapper = new QueryWrapper();
        //wrapper.eq("task_instance_id", taskInstanceId);
        wrapper.eq("process_instance_id", processInstanceId);
        Page<K8sDatasetFile> k8sDatasetFilePage = k8sDatasetFileMapper.selectPage(page, wrapper);
        List<K8sDatasetFile> k8sDatasetFiles = k8sDatasetFilePage.getRecords();
        List<K8sDatasetFileResponse> responseList = k8sDatasetFiles.stream().map(x -> {
            K8sDatasetFileResponse response = new K8sDatasetFileResponse();
            BeanUtils.copyProperties(x, response);
            return response;
        }).collect(Collectors.toList());
        pageInfo.setTotal((int) k8sDatasetFilePage.getTotal());
        pageInfo.setTotalList(responseList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result create(K8sDatasetFileRequest request) {
        Result result = new Result();
        K8sDatasetFile k8sDatasetFile = new K8sDatasetFile();
        BeanUtils.copyProperties(request, k8sDatasetFile);
        k8sDatasetFile.setCreateTime(new Date());
        k8sDatasetFile.setUpdateTime(new Date());
        k8sDatasetFileMapper.insert(k8sDatasetFile);
        result.setData(k8sDatasetFile);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result batchCreate(List<K8sDatasetFileRequest> requests) {
        Result result = new Result();
        for (K8sDatasetFileRequest request : requests) {
            create(request);
        }
        putMsg(result, Status.SUCCESS);
        return result;
    }
}
