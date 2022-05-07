package org.apache.dolphinscheduler.plugin.task.api.k8s;

/**
 * spark operator扩展参数
 *
 * @author lizu
 * @since 2022/4/30
 */
public class K8sSparkOperatorTaskMainParameters extends K8sTaskMainParameters {

    /**
     * 没有认证的k8s 连接
     */
    private String masterUrl;

    private String programType;

    private String mainClass;

    private String mainApplicationFile;

    private String sparkVersion;


    /**
     * A Spark driver pod need a Kubernetes service account in the pod's namespace that has
     * permissions to create, get, list, and delete executor pods, and create a Kubernetes headless
     * service for the driver. The driver will fail and exit without the service account, unless the
     * default service account in the pod's namespace has the needed permissions. To submit and run
     * a SparkApplication in a namespace, please make sure there is a service account with the
     * permissions in the namespace and set
     */
    private String serviceAccount;

    /**
     * driver-cores Number of cores used by the driver, only in cluster mode
     */
    private int driverCores;

    /**
     * driver-memory Memory for driver
     */

    private String driverMemory;

    /**
     * num-executors Number of executors to launch
     */
    private int numExecutors;

    /**
     * executor-cores Number of cores per executor
     */
    private int executorCores;

    /**
     * Memory per executor
     */
    private String executorMemory;

    public String getProgramType() {
        return programType;
    }

    public void setProgramType(String programType) {
        this.programType = programType;
    }

    public int getDriverCores() {
        return driverCores;
    }

    public void setDriverCores(int driverCores) {
        this.driverCores = driverCores;
    }

    public String getDriverMemory() {
        return driverMemory;
    }

    public void setDriverMemory(String driverMemory) {
        this.driverMemory = driverMemory;
    }

    public int getNumExecutors() {
        return numExecutors;
    }

    public void setNumExecutors(int numExecutors) {
        this.numExecutors = numExecutors;
    }

    public int getExecutorCores() {
        return executorCores;
    }

    public void setExecutorCores(int executorCores) {
        this.executorCores = executorCores;
    }

    public String getExecutorMemory() {
        return executorMemory;
    }

    public void setExecutorMemory(String executorMemory) {
        this.executorMemory = executorMemory;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }


    public String getSparkVersion() {
        return sparkVersion;
    }

    public void setSparkVersion(String sparkVersion) {
        this.sparkVersion = sparkVersion;
    }

    public String getMainApplicationFile() {
        return mainApplicationFile;
    }

    public void setMainApplicationFile(String mainApplicationFile) {
        this.mainApplicationFile = mainApplicationFile;
    }

    public String getServiceAccount() {
        return serviceAccount;
    }

    public void setServiceAccount(String serviceAccount) {
        this.serviceAccount = serviceAccount;
    }
}
