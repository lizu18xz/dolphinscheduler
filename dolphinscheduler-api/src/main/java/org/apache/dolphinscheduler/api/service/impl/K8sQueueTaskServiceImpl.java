package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueTaskRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueTaskResponse;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.K8sQueueService;
import org.apache.dolphinscheduler.api.service.K8sQueueTaskService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.K8sQueue;
import org.apache.dolphinscheduler.dao.entity.K8sQueueTask;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.K8sQueueTaskMapper;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Lazy
@Slf4j
public class K8sQueueTaskServiceImpl extends BaseServiceImpl implements K8sQueueTaskService {

    @Autowired
    private K8sQueueTaskMapper k8sQueueTaskMapper;

    @Autowired
    private K8sQueueService k8sQueueService;

    @Override
    public Result createOrUpdateK8sQueueTask(K8sQueueTaskRequest request) {
        //判断是否存在
        Long code = request.getCode();
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("code", code);
        List<K8sQueueTask> k8sQueueTasks = k8sQueueTaskMapper.selectByMap(columnMap);
        if (CollectionUtils.isEmpty(k8sQueueTasks)) {
            K8sQueueTask k8sQueueTask = new K8sQueueTask();
            BeanUtils.copyProperties(request, k8sQueueTask);
            k8sQueueTask.setCreateTime(new Date());
            k8sQueueTaskMapper.insert(k8sQueueTask);
        } else {
            K8sQueueTask k8sQueueTask = k8sQueueTasks.get(0);
            k8sQueueTask.setPriority(request.getPriority());
            k8sQueueTask.setTaskName(request.getTaskName());
            k8sQueueTask.setFlowName(request.getFlowName());
            k8sQueueTask.setTaskType(request.getTaskType());
            k8sQueueTask.setTaskResourceInfo(request.getTaskResourceInfo());
            k8sQueueTask.setCreateTime(new Date());
            k8sQueueTaskMapper.updateById(k8sQueueTask);
        }
        return Result.success();
    }

    @Override
    public Result<PageInfo<K8sQueueTaskResponse>> queryK8sQueueTaskListPaging(User loginUser, Integer pageSize,
                                                                              Integer pageNo, String projectName) {
        Result<PageInfo<K8sQueueTaskResponse>> result = new Result();
        PageInfo<K8sQueueTaskResponse> pageInfo = new PageInfo<>(pageNo, pageSize);
        Page<K8sQueueTask> page = new Page<>(pageNo, pageSize);
        QueryWrapper<K8sQueueTask> wrapper = new QueryWrapper();
        wrapper.eq("project_name", projectName);
        Page<K8sQueueTask> k8sQueueTaskPage = k8sQueueTaskMapper.selectPage(page, wrapper);
        List<K8sQueueTask> projectList = k8sQueueTaskPage.getRecords();

        String queueName = projectName;
        K8sQueue byName = k8sQueueService.findByName(queueName);
        String resourceInfo;
        if (byName != null) {
            resourceInfo = byName.getResourceInfo();
        } else {
            resourceInfo = "";
        }
        List<K8sQueueTaskResponse> responseList = projectList.stream().map(x -> {
            K8sQueueTaskResponse response = new K8sQueueTaskResponse();
            BeanUtils.copyProperties(x, response);
            response.setResourceInfo(resourceInfo);
            return response;
        }).collect(Collectors.toList());

        pageInfo.setTotal((int) k8sQueueTaskPage.getTotal());
        pageInfo.setTotalList(responseList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }


}
