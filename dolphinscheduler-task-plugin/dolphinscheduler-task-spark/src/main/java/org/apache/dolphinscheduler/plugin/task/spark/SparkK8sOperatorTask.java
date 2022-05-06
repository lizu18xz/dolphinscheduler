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
        K8sSparkOperatorTaskMainParameters k8sTaskMainParameters = new K8sSparkOperatorTaskMainParameters();
        Map<String, Property> paramsMap = ParamUtils.convert(taskExecutionContext, getParameters());
        if (MapUtils.isEmpty(paramsMap)) {
            paramsMap = new HashMap<>();
        }
        if (MapUtils.isNotEmpty(taskExecutionContext.getParamsMap())) {
            paramsMap.putAll(taskExecutionContext.getParamsMap());
        }

        k8sTaskMainParameters.setNamespaceName("spark-operator");
        k8sTaskMainParameters.setClusterName("cluster");
        //镜像名称
        k8sTaskMainParameters.setSparkVersion("3.0.0");
        k8sTaskMainParameters.setImage("registry.cn-hangzhou.aliyuncs.com/terminus/spark:v3.0.0");
        k8sTaskMainParameters.setMasterUrl("https://kubernetes.docker.internal:6443");
        k8sTaskMainParameters.setMainClass("org.apache.spark.examples.SparkPi");
        k8sTaskMainParameters.setMainJar("local:///opt/spark/examples/jars/spark-examples_2.12-3.0.0.jar");
        k8sTaskMainParameters.setDriverCores(1);
        k8sTaskMainParameters.setDriverMemory("512m");
        k8sTaskMainParameters.setExecutorCores(1);
        k8sTaskMainParameters.setExecutorMemory("512m");
        k8sTaskMainParameters.setNumExecutors(1);
        k8sTaskMainParameters.setProgramType("Scala");

        k8sTaskMainParameters.setParamsMap(ParamUtils.convert(paramsMap));
        return JSONUtils.toJsonString(k8sTaskMainParameters);
    }
}
