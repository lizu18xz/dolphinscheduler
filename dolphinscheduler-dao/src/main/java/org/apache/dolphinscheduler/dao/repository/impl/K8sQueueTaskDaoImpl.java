/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.dao.repository.impl;

import lombok.NonNull;
import org.apache.dolphinscheduler.dao.entity.K8sQueueTask;
import org.apache.dolphinscheduler.dao.entity.Project;
import org.apache.dolphinscheduler.dao.mapper.K8sQueueTaskMapper;
import org.apache.dolphinscheduler.dao.mapper.ProjectMapper;
import org.apache.dolphinscheduler.dao.repository.BaseDao;
import org.apache.dolphinscheduler.dao.repository.K8sQueueTaskDao;
import org.apache.dolphinscheduler.dao.repository.ProjectDao;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class K8sQueueTaskDaoImpl extends BaseDao<K8sQueueTask, K8sQueueTaskMapper> implements K8sQueueTaskDao {

    public K8sQueueTaskDaoImpl(@NonNull K8sQueueTaskMapper k8sQueueTaskMapper) {
        super(k8sQueueTaskMapper);
    }

    @Override
    public void updateStatus(long taskCode, String status) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("code", taskCode);
        List<K8sQueueTask> k8sQueueTasks = mybatisMapper.selectByMap(columnMap);
        if (!CollectionUtils.isEmpty(k8sQueueTasks)) {
            K8sQueueTask k8sQueueTask = k8sQueueTasks.get(0);
            k8sQueueTask.setTaskStatus(status);
            mybatisMapper.updateById(k8sQueueTask);
        }
    }

    @Override
    public void updateByStatusAndTaskInstanceId(long taskCode, String status, int taskInstanceId) {
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("code", taskCode);
        List<K8sQueueTask> k8sQueueTasks = mybatisMapper.selectByMap(columnMap);
        if (!CollectionUtils.isEmpty(k8sQueueTasks)) {
            K8sQueueTask k8sQueueTask = k8sQueueTasks.get(0);
            k8sQueueTask.setTaskStatus(status);
            k8sQueueTask.setTaskInstanceId(taskInstanceId);
            mybatisMapper.updateById(k8sQueueTask);
        }
    }
}
