package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import io.fabric8.kubernetes.client.utils.Serialization;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.api.dto.k8squeue.*;
import org.apache.dolphinscheduler.api.dto.project.ProjectQueueResourceInfo;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.k8s.K8sClientService;
import org.apache.dolphinscheduler.api.service.K8sQueueService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.common.constants.Constants;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.entity.K8sQueue;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.K8sQueueMapper;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.dolphinscheduler.common.constants.Constants.K8S_GPU_TYPE;

@Service
@Lazy
@Slf4j
public class K8sQueueServiceImpl extends BaseServiceImpl implements K8sQueueService {
    @Autowired
    private K8sQueueMapper k8sQueueMapper;

    @Autowired
    private K8sClientService k8sClientService;

    @Override
    public Result createK8sQueue(K8sQueueRequest request) {
        Result result = new Result();
        if (StringUtils.isEmpty(request.getName())) {
            request.setName(request.getProjectName());
        }

        //获取队列资源信息
        ProjectQueueResourceInfo resourceInfo = request.getProjectQueueResourceInfo();
        if (resourceInfo.getAllocatedCpu() != null && resourceInfo.getAllocatedCpu() < 0.0) {
            log.warn("Parameter capabilityCpu is invalid.");
            putMsg(result, Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.LIMITS_CPU);
            return result;
        }
        if (resourceInfo.getAllocatedMemory() != null && resourceInfo.getAllocatedMemory() < 0.0) {
            log.warn("Parameter capabilityMemory is invalid.");
            putMsg(result, Status.REQUEST_PARAMS_NOT_VALID_ERROR, Constants.LIMITS_MEMORY);
            return result;
        }
        if (request.getWeight() == null) {
            request.setWeight(1);
        }
        request.setReclaimable(false);
        //先查询是否已经存在
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("name", request.getName());
        List<K8sQueue> k8sQueues = k8sQueueMapper.selectByMap(columnMap);
        if (!CollectionUtils.isEmpty(k8sQueues)) {
            log.warn("queue {} already exists.", request.getName());
            putMsg(result, Status.K8S_QUEUE_EXISTS, request.getName());
            return result;
        }
        //先连接k8s集群创建
        if (!Constants.K8S_LOCAL_TEST_CLUSTER_CODE.equals(request.getClusterCode())) {
            try {
                String yamlStr = genDefaultResourceYaml(request);
                log.info("queue yml :{}", yamlStr);
                k8sClientService.loadApplyYmlJob(yamlStr, request.getClusterCode());
            } catch (Exception e) {
                log.error("queue yml create to k8s error", e);
                putMsg(result, Status.K8S_CLIENT_OPS_ERROR, e.getMessage());
                return result;
            }
        }

        //入库
        K8sQueue k8sQueue = new K8sQueue();
        BeanUtils.copyProperties(request, k8sQueue);
        //设置资源json信息
        k8sQueue.setResourceInfo(JSONUtils.toJsonString(resourceInfo));
        k8sQueue.setCreateTime(new Date());
        k8sQueue.setUpdateTime(new Date());
        k8sQueueMapper.insert(k8sQueue);
        result.setData(k8sQueue);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result updateK8sQueue(Long id, K8sQueueRequest request) {
        Result result = new Result();
        K8sQueue k8sQueue = k8sQueueMapper.selectById(id);
        //先删除，在更新集群
        //更新配置
        BeanUtils.copyProperties(request, k8sQueue);
        k8sQueueMapper.updateById(k8sQueue);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result deleteK8sQueue(Long id) {
        Result result = new Result();
        K8sQueue k8sQueue = k8sQueueMapper.selectById(id);
        //删除k8s集群中的队列
        try {
            K8sQueueRequest request = new K8sQueueRequest();
            BeanUtils.copyProperties(k8sQueue, request);
            String yamlStr = genDefaultResourceYaml(request);
            k8sClientService.deleteApplyYmlJob(yamlStr, k8sQueue.getClusterCode());
        } catch (Exception e) {
            log.error("queue yml delete to k8s error", e);
            putMsg(result, Status.K8S_CLIENT_OPS_ERROR, e.getMessage());
            return result;
        }
        k8sQueueMapper.deleteById(id);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result<PageInfo<K8sQueueResponse>> queryK8sQueueListPaging(User loginUser, Integer pageSize, Integer pageNo, String searchVal) {
        Result<PageInfo<K8sQueueResponse>> result = new Result();
        PageInfo<K8sQueueResponse> pageInfo = new PageInfo<>(pageNo, pageSize);
        Page<K8sQueue> page = new Page<>(pageNo, pageSize);
        QueryWrapper<K8sQueue> wrapper = new QueryWrapper();
        wrapper.eq("project_name", searchVal);
        Page<K8sQueue> k8sQueueTaskPage = k8sQueueMapper.selectPage(page, wrapper);
        List<K8sQueue> projectList = k8sQueueTaskPage.getRecords();
        List<K8sQueueResponse> responseList = projectList.stream().map(x -> {
            K8sQueueResponse response = new K8sQueueResponse();
            BeanUtils.copyProperties(x, response);
            response.setQueue(x.getName());
            return response;
        }).collect(Collectors.toList());

        pageInfo.setTotal((int) k8sQueueTaskPage.getTotal());
        pageInfo.setTotalList(responseList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public K8sQueue findByName(String name) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("name", name);
        List<K8sQueue> k8sQueues = k8sQueueMapper.selectByMap(columnMap);
        if (CollectionUtils.isEmpty(k8sQueues)) {
            return null;
        }
        return k8sQueues.get(0);
    }

    @Override
    public Result<List<String>> getGpuType() {
        String gpuType =
                PropertyUtils.getString(K8S_GPU_TYPE);
        String[] split = gpuType.split(",");
        List<String> res = new ArrayList<>();
        for (String type : split) {
            res.add(type);
        }
        return Result.success(res);
    }

    private String genDefaultResourceYaml(K8sQueueRequest request) {
        String name = request.getName();
        ProjectQueueResourceInfo projectQueueResourceInfo = request.getProjectQueueResourceInfo();

        K8sQueueInfo queueInfo = new K8sQueueInfo();
        queueInfo.setApiVersion("scheduling.volcano.sh/v1beta1");
        queueInfo.setKind("Queue");
        K8sQueueInfo.Metadata metadata = new K8sQueueInfo.Metadata();
        metadata.setName(name);
        queueInfo.setMetadata(metadata);
        K8sQueueInfo.Spec spec = new K8sQueueInfo.Spec();
        spec.setReclaimable(false);
        Map<String, Object> deserved = new HashMap<>();

        if (projectQueueResourceInfo.getAllocatedCpu() != null) {
            deserved.put("cpu", projectQueueResourceInfo.getAllocatedCpu());
        }
        if (projectQueueResourceInfo.getAllocatedMemory() != null) {
            deserved.put("memory", projectQueueResourceInfo.getAllocatedMemory());
        }
        if (!StringUtils.isEmpty(projectQueueResourceInfo.getHighGpuName())) {
            deserved.put(projectQueueResourceInfo.getHighGpuName(), projectQueueResourceInfo.getHighAllocatedGpu());
        }

        if (!StringUtils.isEmpty(projectQueueResourceInfo.getLowGpuName())) {
            deserved.put(projectQueueResourceInfo.getLowGpuName(), projectQueueResourceInfo.getLowAllocatedGpu());
        }
        spec.setDeserved(deserved);
        queueInfo.setSpec(spec);
        return Serialization.asYaml(queueInfo);
    }


}
