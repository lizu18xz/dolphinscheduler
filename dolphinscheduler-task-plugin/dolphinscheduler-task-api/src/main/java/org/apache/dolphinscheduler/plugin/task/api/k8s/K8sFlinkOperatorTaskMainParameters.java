package org.apache.dolphinscheduler.plugin.task.api.k8s;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

/**
 * @author lizu
 * @since 2023/9/1
 */
@Data
public class K8sFlinkOperatorTaskMainParameters extends K8sTaskMainParameters {


    private String flinkJobType;

    /**
     * major jar
     */
    private ResourceInfo mainJar;

    /**
     * major class
     */
    private String mainClass;

    /**
     * slot count
     */
    private Integer slot;

    /**
     * parallelism
     */
    private Integer parallelism;

    /**
     * job manager memory
     */
    private String jobManagerMemory;

    /**
     * job manager cpu
     */
    private Double jobManagerCpu;
    /**
     * task manager memory
     */
    private String taskManagerMemory;

    /**
     * task manager cpu
     */
    private Double taskManagerCpu;


    /**
     * 任务配置内容
     * */
    private String rawScript;

    /**
     * resource list
     */
    private List<ResourceInfo> resourceList = new ArrayList<>();

    /**
     * flink version
     */
    private String flinkVersion;

    private String programType;

}
