package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.processTemplate.ProcessTemplateRequest;
import org.apache.dolphinscheduler.api.dto.processTemplate.ProcessTemplateResponse;
import org.apache.dolphinscheduler.api.utils.PageInfo;
import org.apache.dolphinscheduler.api.utils.Result;
import org.apache.dolphinscheduler.dao.entity.User;

public interface ProcessTemplateService {
    Result create(ProcessTemplateRequest request);

    Result<PageInfo<ProcessTemplateResponse>> queryListPaging(User loginUser, Integer pageSize, Integer pageNo, String keyword);
}
