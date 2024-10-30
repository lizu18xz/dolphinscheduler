DROP TABLE IF EXISTS `t_ds_project`;
CREATE TABLE `t_ds_project`
(
    `id`              int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
    `name`            varchar(255) DEFAULT NULL COMMENT 'project name',
    `project_en_name` varchar(255) DEFAULT NULL COMMENT 'project_en_name',
    `code`            bigint(20) NOT NULL COMMENT 'encoding',
    `description`     varchar(255) DEFAULT NULL,
    `user_id`         int(11) DEFAULT NULL COMMENT 'creator id',
    `flag`            tinyint(4) DEFAULT '1' COMMENT '0 not available, 1 available',
    `create_time`     datetime NOT NULL COMMENT 'create time',
    `update_time`     datetime     DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`),
    KEY               `user_id_index` (`user_id`) USING BTREE,
    UNIQUE KEY `unique_name`(`name`),
    UNIQUE KEY `unique_code`(`code`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;


DROP TABLE IF EXISTS `t_ds_k8s_queue`;
CREATE TABLE `t_ds_k8s_queue`
(
    `id`            int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
    `name`          varchar(255) DEFAULT NULL COMMENT 'queue name',
    `project_name`  varchar(255) DEFAULT NULL COMMENT '所属项目',
    `weight`        int(11) NOT NULL COMMENT 'weight',
    `resource_info` text         DEFAULT NULL COMMENT '队列资源信息json',
    `reclaimable`   tinyint(1) NOT NULL COMMENT 'reclaimable',
    `state`         varchar(32)  DEFAULT NULL COMMENT '队列状态',
    `cluster_code`  bigint(20) NOT NULL COMMENT '集群ID',
    `create_time`   datetime NOT NULL COMMENT 'create time',
    `update_time`   datetime     DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_name`(`name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;


DROP TABLE IF EXISTS `t_ds_k8s_queue_task`;
CREATE TABLE `t_ds_k8s_queue_task`
(
    `id`                 int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
    `name`               varchar(255) DEFAULT NULL COMMENT 'queue name',
    `project_name`       varchar(255) DEFAULT NULL COMMENT 'queue name',
    `code`               bigint(20) NOT NULL COMMENT 'task的 标识 code',
    `flow_name`          varchar(255) DEFAULT NULL COMMENT 'flowName',
    `task_name`          varchar(255) DEFAULT NULL COMMENT 'taskName',
    `task_type`          varchar(255) DEFAULT NULL COMMENT 'taskType',
    `name_space`         varchar(255) DEFAULT NULL COMMENT 'nameSpace',
    `task_instance_id`   int(11) DEFAULT NULL COMMENT 'taskInstanceId',
    `priority`           int(11) NOT NULL COMMENT 'priority',
    `task_resource_info` text         DEFAULT NULL COMMENT '任务资源信息json',
    `task_status`        varchar(255) DEFAULT NULL COMMENT 'task_status 运行状态',
    `create_time`        datetime NOT NULL COMMENT 'create time',
    `update_time`        datetime     DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_name`(`name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;


DROP TABLE IF EXISTS `t_ds_k8s_dataset_file`;
CREATE TABLE `t_ds_k8s_dataset_file`
(
    `id`                  int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
    `name`                varchar(255) DEFAULT NULL COMMENT '文件 name',
    `task_instance_id`    int(11) DEFAULT NULL COMMENT 'taskInstanceId',
    `process_instance_id` int(11) DEFAULT NULL COMMENT 'processInstanceId',
    `status`              varchar(255) DEFAULT NULL COMMENT '文件处理状态',
    `create_time`         datetime NOT NULL COMMENT 'create time',
    `update_time`         datetime     DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;


DROP TABLE IF EXISTS `t_ds_process_template`;
CREATE TABLE `t_ds_process_template`
(
    `id`          int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
    `process_id`  int(11) DEFAULT NULL COMMENT '流程ID',
    `name`        varchar(255) DEFAULT NULL COMMENT '流程名称',
    `code`               bigint(20) NOT NULL COMMENT '流程 标识 code',
    `project_code`               bigint(20) NOT NULL COMMENT '所属项目 code',
    `create_time` datetime NOT NULL COMMENT 'create time',
    `update_time` datetime     DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;



