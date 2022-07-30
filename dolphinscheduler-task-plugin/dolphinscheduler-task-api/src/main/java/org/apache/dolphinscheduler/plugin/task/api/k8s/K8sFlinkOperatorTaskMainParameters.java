package org.apache.dolphinscheduler.plugin.task.api.k8s;

import java.util.ArrayList;
import java.util.List;
import org.apache.dolphinscheduler.plugin.task.api.model.ResourceInfo;

/**
 * @author lizu
 * @since 2022/7/30
 */
public class K8sFlinkOperatorTaskMainParameters extends K8sTaskMainParameters {

    /**
     * 没有认证的k8s 连接
     */
    private String masterUrl;

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
    private int slot;

    /**
     * parallelism
     */
    private int parallelism;

    /**
     * job manager memory
     */
    private String jobManagerMemory;

    /**
     * task manager memory
     */
    private String taskManagerMemory;

    /**
     * resource list
     */
    private List<ResourceInfo> resourceList = new ArrayList<>();

    /**
     * flink version
     */
    private String flinkVersion;

    private String programType;

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public ResourceInfo getMainJar() {
        return mainJar;
    }

    public void setMainJar(ResourceInfo mainJar) {
        this.mainJar = mainJar;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public int getSlot() {
        return slot;
    }

    public void setSlot(int slot) {
        this.slot = slot;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getJobManagerMemory() {
        return jobManagerMemory;
    }

    public void setJobManagerMemory(String jobManagerMemory) {
        this.jobManagerMemory = jobManagerMemory;
    }

    public String getTaskManagerMemory() {
        return taskManagerMemory;
    }

    public void setTaskManagerMemory(String taskManagerMemory) {
        this.taskManagerMemory = taskManagerMemory;
    }

    public List<ResourceInfo> getResourceList() {
        return resourceList;
    }

    public void setResourceList(
        List<ResourceInfo> resourceList) {
        this.resourceList = resourceList;
    }

    public String getFlinkVersion() {
        return flinkVersion;
    }

    public void setFlinkVersion(String flinkVersion) {
        this.flinkVersion = flinkVersion;
    }

    public String getProgramType() {
        return programType;
    }

    public void setProgramType(String programType) {
        this.programType = programType;
    }
}
