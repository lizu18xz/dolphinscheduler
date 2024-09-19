package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import org.apache.dolphinscheduler.dao.entity.K8sQueueTask;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface K8sQueueTaskMapper extends BaseMapper<K8sQueueTask> {

    IPage<K8sQueueTask> queryK8sQueueTaskListPaging(IPage<K8sQueueTask> page,
                                          @Param("projectsIds") List<Integer> projectsIds,
                                          @Param("searchName") String searchName);

}
