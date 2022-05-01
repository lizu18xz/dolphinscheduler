package org.apache.dolphinscheduler.plugin.task.api.k8s.spark;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.Map;

/**
 * @author lizu
 * @since 2022/5/1
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Driver {

    /**
     * TODO 升级后改成 最新的evn传递方式
     */
    private Map<String, Object> envVars;

    private Integer cores;

    private String coreLimit;

    private String memory;

    private Labels labels;

    private String serviceAccount;

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

    public String getCoreLimit() {
        return coreLimit;
    }

    public void setCoreLimit(String coreLimit) {
        this.coreLimit = coreLimit;
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

    public String getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
    }

    public List<VolumeMounts> getVolumeMounts() {
        return volumeMounts;
    }

    public void setVolumeMounts(
        List<VolumeMounts> volumeMounts) {
        this.volumeMounts = volumeMounts;
    }

    public static class Labels {

        private String version;

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

    public static class VolumeMounts {

        private String name;

        private String mountPath;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMountPath() {
            return mountPath;
        }

        public void setMountPath(String mountPath) {
            this.mountPath = mountPath;
        }
    }

}
