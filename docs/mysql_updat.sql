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
    `id`                int(11) NOT NULL AUTO_INCREMENT COMMENT 'key',
    `name`              varchar(255)   DEFAULT NULL COMMENT 'queue name',
    `weight`            int(11) NOT NULL COMMENT 'weight',
    `capability_cpu`    decimal(14, 3) DEFAULT NULL,
    `capability_momory` decimal(14, 3) DEFAULT NULL,
    `create_time`       datetime NOT NULL COMMENT 'create time',
    `update_time`       datetime       DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `unique_name`(`name`),
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;



