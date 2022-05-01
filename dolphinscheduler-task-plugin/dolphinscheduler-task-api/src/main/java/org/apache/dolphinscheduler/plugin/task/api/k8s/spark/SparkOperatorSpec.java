package org.apache.dolphinscheduler.plugin.task.api.k8s.spark;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * @author lizu
 * @since 2022/5/1
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SparkOperatorSpec {

    private String type;

    private String mode;

    private String image;

    private String imagePullPolicy;

    private String mainClass;

    private String mainApplicationFile;

    private String sparkVersion;

    private RestartPolicy restartPolicy;

    private List<Volume> volumes;

    private Driver driver;

    private Executor executor;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getImagePullPolicy() {
        return imagePullPolicy;
    }

    public void setImagePullPolicy(String imagePullPolicy) {
        this.imagePullPolicy = imagePullPolicy;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public String getMainApplicationFile() {
        return mainApplicationFile;
    }

    public void setMainApplicationFile(String mainApplicationFile) {
        this.mainApplicationFile = mainApplicationFile;
    }

    public String getSparkVersion() {
        return sparkVersion;
    }

    public void setSparkVersion(String sparkVersion) {
        this.sparkVersion = sparkVersion;
    }

    public RestartPolicy getRestartPolicy() {
        return restartPolicy;
    }

    public void setRestartPolicy(
        RestartPolicy restartPolicy) {
        this.restartPolicy = restartPolicy;
    }

    public List<Volume> getVolumes() {
        return volumes;
    }

    public void setVolumes(
        List<Volume> volumes) {
        this.volumes = volumes;
    }

    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }
}
