package org.apache.dolphinscheduler.plugin.task.spark;


import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.SPARK_ON_K8S_OPERATOR;

import java.util.HashMap;
import java.util.Map;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sSparkOperatorTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.MapUtils;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

/**
 * @author lizu
 * @since 2022/4/30
 */
public class SparkK8sOperatorTask extends AbstractK8sTask {

    private final TaskExecutionContext taskExecutionContext;

    private SparkParameters sparkParameters;

    /**
     * Abstract k8s Task
     *
     * @param taskRequest taskRequest
     */
    protected SparkK8sOperatorTask(TaskExecutionContext taskRequest) {
        super(taskRequest, SPARK_ON_K8S_OPERATOR);
        this.taskExecutionContext = taskRequest;
        sparkParameters = JSONUtils
            .parseObject(taskExecutionContext.getTaskParams(), SparkParameters.class);
    }

    @Override
    public AbstractParameters getParameters() {
        return sparkParameters;
    }

    /**
     * TODO spark operator参数需从页面配置
     */
    @Override
    protected String buildCommand() {
        K8sSparkOperatorTaskMainParameters k8sSparkOperatorTaskMainParameters = new K8sSparkOperatorTaskMainParameters();
        Map<String, Property> paramsMap = ParamUtils.convert(taskExecutionContext, getParameters());
        if (MapUtils.isEmpty(paramsMap)) {
            paramsMap = new HashMap<>();
        }
        if (MapUtils.isNotEmpty(taskExecutionContext.getParamsMap())) {
            paramsMap.putAll(taskExecutionContext.getParamsMap());
        }

        k8sSparkOperatorTaskMainParameters.setClusterName("cluster");
        //TODO serviceAccount
        k8sSparkOperatorTaskMainParameters.setServiceAccount(sparkParameters.getServiceAccount());
        k8sSparkOperatorTaskMainParameters.setNamespaceName(sparkParameters.getNamespace());
        //镜像名称
        k8sSparkOperatorTaskMainParameters.setSparkVersion(sparkParameters.getSparkVersion());
        k8sSparkOperatorTaskMainParameters
            .setImage(sparkParameters.getImage());
        k8sSparkOperatorTaskMainParameters.setMasterUrl(sparkParameters.getMasterUrl());
        k8sSparkOperatorTaskMainParameters.setMainClass(sparkParameters.getMainClass());
        k8sSparkOperatorTaskMainParameters
            .setMainApplicationFile(sparkParameters.getMainApplicationFile());
        k8sSparkOperatorTaskMainParameters.setDriverCores(sparkParameters.getDriverCores());
        k8sSparkOperatorTaskMainParameters.setDriverMemory(sparkParameters.getDriverMemory());
        k8sSparkOperatorTaskMainParameters.setExecutorCores(sparkParameters.getExecutorCores());
        k8sSparkOperatorTaskMainParameters.setExecutorMemory(sparkParameters.getExecutorMemory());
        k8sSparkOperatorTaskMainParameters.setNumExecutors(sparkParameters.getNumExecutors());
        k8sSparkOperatorTaskMainParameters
            .setProgramType(convertProgramType(sparkParameters.getProgramType()));
        k8sSparkOperatorTaskMainParameters.setParamsMap(ParamUtils.convert(paramsMap));
        return JSONUtils.toJsonString(k8sSparkOperatorTaskMainParameters);
    }

    private String convertProgramType(ProgramType programType) {
        String type;
        switch (programType) {
            case JAVA:
                type = "Java";
                break;
            case SCALA:
                type = "Scala";
                break;
            default:
                type = "Python";
                break;
        }
        return type;
    }
}
