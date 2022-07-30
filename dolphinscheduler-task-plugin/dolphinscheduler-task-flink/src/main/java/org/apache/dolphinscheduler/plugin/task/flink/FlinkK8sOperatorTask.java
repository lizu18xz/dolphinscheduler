package org.apache.dolphinscheduler.plugin.task.flink;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR;

import java.util.HashMap;
import java.util.Map;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sFlinkOperatorTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parser.ParamUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.MapUtils;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

/**
 * @author lizu
 * @since 2022/7/30
 */
public class FlinkK8sOperatorTask extends AbstractK8sTask {

    private FlinkParameters flinkParameters;

    private final TaskExecutionContext taskExecutionContext;

    protected FlinkK8sOperatorTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext, FLINK_K8S_OPERATOR);
        this.taskExecutionContext = taskExecutionContext;
        flinkParameters = JSONUtils
            .parseObject(taskExecutionContext.getTaskParams(), FlinkParameters.class);

    }

    @Override
    public AbstractParameters getParameters() {
        return flinkParameters;
    }

    @Override
    protected String buildCommand() {

        K8sFlinkOperatorTaskMainParameters parameters = new K8sFlinkOperatorTaskMainParameters();

        Map<String, Property> paramsMap = ParamUtils.convert(taskExecutionContext, getParameters());
        if (MapUtils.isEmpty(paramsMap)) {
            paramsMap = new HashMap<>();
        }
        if (MapUtils.isNotEmpty(taskExecutionContext.getParamsMap())) {
            paramsMap.putAll(taskExecutionContext.getParamsMap());
        }

        //TODO 暂时设置这些参数,其他配置到模板中,比如namespace

        parameters.setMasterUrl(flinkParameters.getMasterUrl());
        parameters.setFlinkVersion(flinkParameters.getFlinkVersion());
        parameters.setProgramType(ProgramType.convertProgramType(flinkParameters.getProgramType()));
        parameters.setMainJar(flinkParameters.getMainJar());
        parameters.setMainClass(flinkParameters.getMainClass());
        parameters.setJobManagerMemory(flinkParameters.getJobManagerMemory());
        parameters.setParallelism(flinkParameters.getParallelism());
        parameters.setTaskManagerMemory(flinkParameters.getTaskManagerMemory());
        parameters.setSlot(flinkParameters.getSlot());
        parameters.setResourceList(flinkParameters.getResourceFilesList());
        parameters.setParamsMap(ParamUtils.convert(paramsMap));

        return JSONUtils.toJsonString(parameters);
    }


}
