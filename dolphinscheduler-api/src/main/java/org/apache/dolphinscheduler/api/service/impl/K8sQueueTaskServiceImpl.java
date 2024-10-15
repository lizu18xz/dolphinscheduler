package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueTaskRequest;
import org.apache.dolphinscheduler.api.dto.k8squeue.K8sQueueTaskResponse;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.k8s.K8sClientService;
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

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.*;

@Service
@Lazy
@Slf4j
public class K8sQueueTaskServiceImpl extends BaseServiceImpl implements K8sQueueTaskService {

    @Autowired
    private K8sQueueTaskMapper k8sQueueTaskMapper;

    @Autowired
    private K8sQueueService k8sQueueService;

    @Autowired
    private K8sClientService k8sClientService;

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
            k8sQueueTask.setUpdateTime(new Date());
            k8sQueueTask.setTaskStatus("待运行");
            //初始化的时候没有任务实列ID
            k8sQueueTask.setTaskInstanceId(-1);
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
        List<K8sQueueTask> k8sQueueTasks = k8sQueueTaskPage.getRecords();
        String resourceInfo;
        List<K8sQueueTaskResponse> responseList = new ArrayList<>();

        if (!CollectionUtils.isEmpty(k8sQueueTasks)) {
            //暂时都是一个队列名称
            String queueName = k8sQueueTasks.get(0).getName();

            K8sQueue byName = k8sQueueService.findByName(queueName);
            if (byName != null) {
                resourceInfo = byName.getResourceInfo();
            } else {
                resourceInfo = "";
            }
            Long clusterCode = byName.getClusterCode();
            //获取队列中的状态
            String nameSpace = queueName;
            List<GenericKubernetesResource> items = k8sClientService.getVcJobStatus(nameSpace, clusterCode);
            Map<String, String> collectMap = new HashMap<>();
            if (!CollectionUtils.isEmpty(items)) {
                for (GenericKubernetesResource item : items) {
                    String stateDesc = "";
                    Map<String, Object> additionalProperties = item
                            .getAdditionalProperties();
                    if (additionalProperties != null) {
                        Map<String, Object> status = (Map<String, Object>) additionalProperties
                                .get("status");
                        if (status != null) {
                            Map<String, Object> applicationState = (Map<String, Object>) status
                                    .get("state");
                            if (applicationState != null) {
                                if (applicationState.get("phase") != null) {
                                    String state = applicationState.get("phase").toString();
                                    if (state.equals("Completed")) {
                                        stateDesc = "运行完成";
                                    } else if (state.equals("Failed")) {
                                        stateDesc = "运行失败";
                                    } else if (state.equals("Pending")) {
                                        stateDesc = "排队中";
                                    } else {
                                        stateDesc = "运行中";
                                    }
                                }
                            }
                        }
                    }
                    collectMap.put(item.getMetadata().getName(), stateDesc);
                }
            }
            responseList = k8sQueueTasks.stream().map(x -> {
                K8sQueueTaskResponse response = new K8sQueueTaskResponse();
                BeanUtils.copyProperties(x, response);
                response.setResourceInfo(resourceInfo);
                if (x.getTaskInstanceId() > 0) {
                    String k8sJobName = String.format("%s-%s", x.getTaskName(), x.getTaskInstanceId());
                    response.setTaskInQueueStatus(collectMap.get(k8sJobName));
                }
                return response;
            }).collect(Collectors.toList());
        }
        pageInfo.setTotal((int) k8sQueueTaskPage.getTotal());
        pageInfo.setTotalList(responseList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public void updateK8sQueueTaskStatus(String taskCode, String status) {

    }


}
