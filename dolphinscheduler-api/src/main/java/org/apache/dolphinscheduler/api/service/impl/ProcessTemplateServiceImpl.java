package org.apache.dolphinscheduler.api.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.dolphinscheduler.api.dto.processTemplate.ProcessTemplateRequest;
import org.apache.dolphinscheduler.api.dto.processTemplate.ProcessTemplateResponse;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProcessTemplateService;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.ProcessTemplate;
import org.apache.dolphinscheduler.dao.entity.User;
import org.apache.dolphinscheduler.dao.mapper.ProcessTemplateMapper;
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
public class ProcessTemplateServiceImpl extends BaseServiceImpl implements ProcessTemplateService {

    @Autowired
    private ProcessTemplateMapper processTemplateMapper;


    @Override
    public Result create(ProcessTemplateRequest request) {
        Result result = new Result();
        ProcessTemplate processTemplate = new ProcessTemplate();
        BeanUtils.copyProperties(request, processTemplate);
        processTemplate.setCreateTime(new Date());
        processTemplate.setUpdateTime(new Date());
        processTemplateMapper.insert(processTemplate);
        result.setData(processTemplate);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public Result<PageInfo<ProcessTemplateResponse>> queryListPaging(User loginUser, Integer pageSize, Integer pageNo, String keyword) {
        Result<PageInfo<ProcessTemplateResponse>> result = new Result();
        PageInfo<ProcessTemplateResponse> pageInfo = new PageInfo<>(pageNo, pageSize);
        Page<ProcessTemplate> page = new Page<>(pageNo, pageSize);
        QueryWrapper<ProcessTemplate> wrapper = new QueryWrapper();
        wrapper.eq("name", keyword);
        Page<ProcessTemplate> processTemplatePage = processTemplateMapper.selectPage(page, wrapper);
        List<ProcessTemplate> processTemplates = processTemplatePage.getRecords();
        List<ProcessTemplateResponse> responseList = processTemplates.stream().map(x -> {
            ProcessTemplateResponse response = new ProcessTemplateResponse();
            BeanUtils.copyProperties(x, response);
            return response;
        }).collect(Collectors.toList());
        pageInfo.setTotal((int) processTemplatePage.getTotal());
        pageInfo.setTotalList(responseList);
        result.setData(pageInfo);
        putMsg(result, Status.SUCCESS);
        return result;
    }


}

