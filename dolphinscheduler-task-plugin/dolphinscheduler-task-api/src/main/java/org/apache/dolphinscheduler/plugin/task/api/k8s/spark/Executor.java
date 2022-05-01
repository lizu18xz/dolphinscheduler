package org.apache.dolphinscheduler.plugin.task.api.k8s.spark;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver.Labels;
import org.apache.dolphinscheduler.plugin.task.api.k8s.spark.Driver.VolumeMounts;

/**
 * @author lizu
 * @since 2022/5/1
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Executor {

    /**
     * TODO 升级后改成 最新的evn传递方式
     */
    private Map<String, Object> envVars;

    private Integer cores;

    private Integer instances;

    private String memory;

    private Labels labels;

    private List<VolumeMounts> volumeMounts;

    public Map<String, Object> getEnvVars() {
        return envVars;
    }

    public void setEnvVars(Map<String, Object> envVars) {
        this.envVars = envVars;
    }

    public Integer getCores() {
        return cores;
    }

    public void setCores(Integer cores) {
        this.cores = cores;
    }

    public Integer getInstances() {
        return instances;
    }

    public void setInstances(Integer instances) {
        this.instances = instances;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public Labels getLabels() {
        return labels;
    }

    public void setLabels(Labels labels) {
        this.labels = labels;
    }

    public List<VolumeMounts> getVolumeMounts() {
        return volumeMounts;
    }

    public void setVolumeMounts(
        List<VolumeMounts> volumeMounts) {
        this.volumeMounts = volumeMounts;
    }
}
