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
    PRIMARY KEY (`id`)
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
    `project_code`               bigint(20) NOT NULL COMMENT '项目code',
    `create_time` datetime NOT NULL COMMENT 'create time',
    `update_time` datetime     DEFAULT NULL COMMENT 'update time',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE = utf8_bin;

INSERT INTO `dolphinscheduler`.`t_ds_project` (`id`, `name`, `code`, `description`, `user_id`, `flag`, `create_time`, `update_time`, `project_en_name`) VALUES (24, '演示', 14912393717952, NULL, 1, 1, '2024-09-10 09:57:56', '2024-09-10 09:57:56', 'projecttmp');
INSERT INTO `dolphinscheduler`.`t_ds_k8s_queue` (`id`, `name`, `project_name`, `weight`, `resource_info`, `reclaimable`, `state`, `cluster_code`, `create_time`, `update_time`) VALUES (3, 'projecttmp', 'projecttmp', 1, '{\"allocatedCpu\":32.0,\"allocatedMemory\":32768.0,\"highGpuName\":\"nvidia.com/r3090\",\"highAllocatedGpu\":10,\"lowGpuName\":\"nvidia.com/gpu\",\"lowAllocatedGpu\":2}', 0, NULL, 14591364587200, '2024-10-08 09:44:33', '2024-10-08 09:44:33');
INSERT INTO `dolphinscheduler`.`t_ds_k8s_namespace` (`id`, `code`, `limits_memory`, `namespace`, `user_id`, `pod_replicas`, `pod_request_cpu`, `pod_request_memory`, `limits_cpu`, `cluster_code`, `create_time`, `update_time`) VALUES (2, 15298904747072, 32, 'projecttmp', 1, 0, 0.000, 0, 32.000, 14591364587200, '2024-10-15 08:44:53', '2024-10-18 14:55:36');


INSERT INTO `dolphinscheduler`.`t_ds_cluster` (`id`, `code`, `name`, `config`, `description`, `operator`, `create_time`, `update_time`) VALUES (1, 14591364587200, 'yiqi', '{\"k8s\":\"apiVersion: v1\\nclusters:\\n- cluster:\\n    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURCVENDQWUyZ0F3SUJBZ0lJVzRPMGJUK25tOG93RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TkRBNU1qVXdPREEwTWpOYUZ3MHpOREE1TWpNd09EQTVNak5hTUJVeApFekFSQmdOVkJBTVRDbXQxWW1WeWJtVjBaWE13Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLCkFvSUJBUUN1Vjg1eHV6L2xuMjRVZG1FK3A2eVZsSnZWT3htQXZqVTdETFAvcGVJUEx1cVlrdlFGSHB0eW01NTAKY1BxQ24zdDdNanE1ajFyM1djam9UUDVqOGtDelFXYlRYMUdNd2FuVkxQdG9oejNuMHkzWHRUUGx5Z1ZnNDFoVQpDeEJGVjdsK3ZSUkVJemViRkVwamNJNWRabWZGNzhMTWpOaklYVlduVS9IR200MkxIUk1ZY2YwYm5VWjZkeG1wClRkdEpDY0lEeW83NDE3endSaC9sano0V2gyYkJiZUZTTGVJL1dYbXYySTNWSEs3MFVLdUUya1pQRGZROHJGTDcKdUhHeHpXOGdZQVArd005MFA1aVM2RlFwWVM1dm00b21jYUtBQzdJNG9XOWx6WVk4enV6WmswMTQwWkdNNS96YwpmTFVJOXJIcHpQN3lQUHRpNTcvSXdRSUxnVFkzQWdNQkFBR2pXVEJYTUE0R0ExVWREd0VCL3dRRUF3SUNwREFQCkJnTlZIUk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJRaFYyVnU0aWxRQ1BSVHg1eGpWSjRYYjNRbGN6QVYKQmdOVkhSRUVEakFNZ2dwcmRXSmxjbTVsZEdWek1BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQnpmY1dNQURYVQo5U2kxZ1VyQ2VHYkM1Z1Q2aXdORTJpTzlvZEJxZHdwaWNoeVdJUFhIeG05cStseCtRV3FjbTRTV212QzVYVmFFClNLeHVOS201WEJOMCt2b0tBaXdhSEtOY2JpejJOSkNMZmx2Y2NBUUg3Nk5EdHBtZ2ZXaEIvQ0s0MGxjZy9meE0KdWdpUXJyRmFBS0FONVZWL1ZTV0U3ZVZ4MGhCMmR3ZnYyNHFwN3kxT1dta3RvY0FabU1ObHQ1NlAycVU5WjZOSQpUVFd2MGVpYi9JSzM0Q0cvNXlrOUUwUFE2aGhnaWlxWHBSbS85Q21pd29jV0R3emZuK05XTkxQN2UxNDFLVzZPCk0rRk5mcC9HMUNGMWdYcTAzL1BUbWtVYmNoUklOOUNsK3VIWWx3cnRFYXJHUHlrcFpOc0dWT0Q2bEFFaTdlQUYKRVVPQWJQMnNWNTY1Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K\\n    server: https://10.78.1.189:6443\\n  name: kubernetes\\ncontexts:\\n- context:\\n    cluster: kubernetes\\n    user: kubernetes-admin\\n  name: kubernetes-admin@kubernetes\\ncurrent-context: kubernetes-admin@kubernetes\\nkind: Config\\npreferences: {}\\nusers:\\n- name: kubernetes-admin\\n  user:\\n    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURJVENDQWdtZ0F3SUJBZ0lJU04rOENhZmpuQUV3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TkRBNU1qVXdPREEwTWpOYUZ3MHlOVEE1TWpVd09EQTVNalJhTURReApGekFWQmdOVkJBb1REbk41YzNSbGJUcHRZWE4wWlhKek1Sa3dGd1lEVlFRREV4QnJkV0psY201bGRHVnpMV0ZrCmJXbHVNSUlCSWpBTkJna3Foa2lHOXcwQkFRRUZBQU9DQVE4QU1JSUJDZ0tDQVFFQXYxR0tadTBGYkVMa2phZncKbHZVa1FUWEZTTUY1Kzg1VDRFai9MemhlLzNGWkpIdGRjNzNObDFuZHVnRmJZUUd2VzJhL2JDaXN6UmFQaUFRVQpxVXNMVDVaRmcxZW5VU1R4U3FoNkNEZTI3UFNLSEpsZXY3UnR4d3VtSXZNdmpVTDdjbkJIYWZQUXFqN01WbEZDCkdta0hpM3ArQW9jZ3B4T3ptUGIwUm1MakRoY3pQdSswenhBMHFMdUxWM3JJNHlyRmVCOFA0UExkYXFLUFlBSEcKV3B5cDJMMWVXRitQNk9pUitzdjJKKzAzbWUvd3R1VzBPUFZjaDFEWk5oL3NUSFZzWGhBRTFxVnAySVJDREtmOQpUeEZMcnZvQi9GQytvTXFxSjFXODhoWGVrQjh5TDNqb1NBOTJaSlJ1aTRtNnBBTE1OY2MvTTFXM3l3TER0Si9kCkhZWm44UUlEQVFBQm8xWXdWREFPQmdOVkhROEJBZjhFQkFNQ0JhQXdFd1lEVlIwbEJBd3dDZ1lJS3dZQkJRVUgKQXdJd0RBWURWUjBUQVFIL0JBSXdBREFmQmdOVkhTTUVHREFXZ0JRaFYyVnU0aWxRQ1BSVHg1eGpWSjRYYjNRbApjekFOQmdrcWhraUc5dzBCQVFzRkFBT0NBUUVBVUVHeXhsbWRvSmJGVFpZNTFwMGJjeEtqR0tIWHg2WW8vczNaCml3dldMOEVwVENQSlBHaFFvRHpUK2FkSlVQRVlEWVNqUHJVcnQrbUgzbFZURWdKa0VBU0hDTmEwa0p1VnlYbkEKdnZYcUNMWExwdUZEdG1NSG9VNmpmYWdrdTNxRmRqSHpBWlJCRG9JUW05ZUo4V0tjTEdIM2YrODJYZVE2ZFByQgp6N0tydEpZV2ZmakxOZWR4YzBnTjdvUWJvYW11K25CYlRhOUM2Mm02YXArUFdla3VZOXZtRGJidTVvbEtqVmdHClg1eXpIUngzRjJxYVR1YlV2dThSZWRrZlJiVmU2Z3hGTlgwSnU1NGVBNzFENlluc2RMa0ZQcWRVcVErVUN1V1EKeVVtYlBhY3BlcFBJNnl2bUhqa1V0S1VSUENMUlF4K0RxQXd5NGo2NzIvZzh3L0M2R0E9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==\\n    client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb3dJQkFBS0NBUUVBdjFHS1p1MEZiRUxramFmd2x2VWtRVFhGU01GNSs4NVQ0RWovTHpoZS8zRlpKSHRkCmM3M05sMW5kdWdGYllRR3ZXMmEvYkNpc3pSYVBpQVFVcVVzTFQ1WkZnMWVuVVNUeFNxaDZDRGUyN1BTS0hKbGUKdjdSdHh3dW1Jdk12alVMN2NuQkhhZlBRcWo3TVZsRkNHbWtIaTNwK0FvY2dweE96bVBiMFJtTGpEaGN6UHUrMAp6eEEwcUx1TFYzckk0eXJGZUI4UDRQTGRhcUtQWUFIR1dweXAyTDFlV0YrUDZPaVIrc3YySiswM21lL3d0dVcwCk9QVmNoMURaTmgvc1RIVnNYaEFFMXFWcDJJUkNES2Y5VHhGTHJ2b0IvRkMrb01xcUoxVzg4aFhla0I4eUwzam8KU0E5MlpKUnVpNG02cEFMTU5jYy9NMVczeXdMRHRKL2RIWVpuOFFJREFRQUJBb0lCQURZSGhScUVIVi94bERJZApGMmpLV2k3QVRMSzdVUU8zeFNBMS9Uc01wR2RuVDFGcU5YNFdRQlZhSDdBRDZoWi9MMUtVdGNLSnlpTXhGOS9GCnAwQVlIVjJnaG9rTkhBZGljM0l1R2lodjg0ei9rbkY1THJUYzg1T2V5cEJjTlRXamtQZEVsaVpwNEtmempDbGsKVVZLOERwRnBTbVRVWEhoU3JBbXExRmw4VzdzQ2ZGOTA0RnB0a2FWNlF5Ky9wbGJ5Q3JEbTUxSEN3NGpGQ1EwNgpZU2s4NzBsck1HZHZ0MjNlcCtuNzc3bTFTcVZleWJvMUE0ZlBKR3pDbnAwK1V5SjhXOHdqeDFSNWg0YjlwYnFtCkgwL1hMQzFXYlVJM3NsTi8wNHJWWDQ5YUYydTIrdXdPMUV4enFVbG5kaDFaL0F4WUlsUEVwL3lxcFBhaHpzdUIKRWFQNzluRUNnWUVBeEE3VU91M2JoMXBoME1uSG53SXZPYS9zWEJSc1JKYTFSeEJSSFc1ZmlUTGNOUE5sSVZ6Nwo1OFp6WU4xQnFtWHFYQ1BvdDVjMTBCZTRBSDUya3FEeFZocm9WUnBFSU53MDFFZHFKa21XNkdxVmd6MGEyOEZqCnp2dnlrWThFSVFxRXJZdUh5eDVMZUljNkNMN3poN2RDdHcyNzVONHpqczVKNXp6YSs4U1NYV3NDZ1lFQStjL0QKMXcrTEp4aEwvNU1IQXRaTWsvd3ZERFAxa04zOUZMYitoeDkva1lubnFrVmlkb3dQeGVvbmJjUGc0ZDlvQlRVQgpITVFEcDBWbFBCWmVmSzVZUEc1dFM3TkxZQnJ5QnJEK2FGYml2Q3ZZczczZWtuQU5vRkhUeUhPVzRBVnVtMlN0CnFqUGtIT043N3NkVVNOdUtmUjQxeXRtcFc1Z1pPTXFLY0V5SnF4TUNnWUJsUlRPaTdOSUViRTh5UmNmeS9uSGUKWGx5OGcyQVpYYTl0Y2FRMGk1cVZoOHZ2SGZvUzdiREEyK1VsRjZTZm05MGVrdjNXTnlqNHdBZXZXYU50d2V4bgpDYWNRcm15YWZLUmRNalpHYUtTbWtNZUI3c0k4MlB2eElucjliTjQ1SXZHOW91RVZyaVJWc2FQc2VKWGFlSExoCmt4cUc4YSs0Q0I1c2J5YTkvWitNMlFLQmdRRDRYUFRqSzBQNDBYSTFuWEs2QWw5T1BjcDB3bS9oNEtLT1pzVTEKaUQ1bmJ3a1JReng2aWhQZ2ZFVkpIeGJFMmoxcjk5K3NQc0UzRzRwanJBV3l4Zzladlp3U1NDYW5YUUxGWGxFQgpZV2k1Rm9xellSRVkzQ2pmL0pxblR5eHdlMVlWVG9wT2pwcTdrZnVVVVQ3ZDFNK2lSMWZKM284L1RqKzlNN2xoCm51YThXd0tCZ0dob3RlMTExWW8waU9ORFNJTEozS3MrRmYyaW9BK0liL2tDeTEzaDN1NWJ6MGwvT3pxM2lZMjIKQWpkRU80NmtUZ2tMNmxyQ0NTQmpSUlJJMCtLcTdOdTBUVThBNnpiazBZRk04KytIN3VPZGlQT0tSYldCSUUrUgpKN3FITW52N0huMGlCZzBKbmZHeWZ0V1BHVHNuMVVhYk1hK0JNR3dkc0dkdENKQzNrVHFnCi0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg==\",\"yarn\":\"\"}', 'yiqi测试', 1, '2024-08-12 09:17:16', '2024-08-12 09:17:16');


INSERT INTO `dolphinscheduler`.`t_ds_access_token` (`id`, `user_id`, `token`, `expire_time`, `create_time`, `update_time`) VALUES (1, 1, 'b75b3feb300214911cfcfdbc96963fa0', '2099-09-05 10:00:18', '2024-09-05 10:00:38', '2024-09-05 10:00:38');
