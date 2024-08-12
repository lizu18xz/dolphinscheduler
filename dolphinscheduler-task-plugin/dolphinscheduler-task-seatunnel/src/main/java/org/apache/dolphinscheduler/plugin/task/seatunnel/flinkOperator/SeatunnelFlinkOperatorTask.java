package org.apache.dolphinscheduler.plugin.task.seatunnel.flinkOperator;

import static org.apache.dolphinscheduler.plugin.task.api.TaskConstants.FLINK_K8S_OPERATOR;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sTask;
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sFlinkOperatorTaskMainParameters;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author lizu
 * @since 2023/9/1
 */
public class SeatunnelFlinkOperatorTask extends AbstractK8sTask {

    private SeatunnelFlinkOperatorParameters seatunnelFlinkOperatorParameters;

    private final TaskExecutionContext taskExecutionContext;

    /**
     * Abstract k8s Task
     *
     * @param taskExecutionContext
     */
    public SeatunnelFlinkOperatorTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext, FLINK_K8S_OPERATOR);
        this.taskExecutionContext = taskExecutionContext;
        seatunnelFlinkOperatorParameters = JSONUtils
                .parseObject(taskExecutionContext.getTaskParams(),
                        SeatunnelFlinkOperatorParameters.class);
    }

    @Override
    public List<String> getApplicationIds() throws TaskException {
        return Collections.emptyList();
    }

    @Override
    public AbstractParameters getParameters() {
        return seatunnelFlinkOperatorParameters;
    }

    /**
     * 提交的yml文件相关配置
     */
    @Override
    protected String buildCommand() {
        K8sFlinkOperatorTaskMainParameters parameters = new K8sFlinkOperatorTaskMainParameters();
        parameters.setFlinkJobType(TaskConstants.FLINK_SEATUNNEL);
        Map<String, Property> paramsMap = taskExecutionContext.getPrepareParamsMap();

        parameters.setImage(seatunnelFlinkOperatorParameters.getImage());
        parameters.setImagePullPolicy(seatunnelFlinkOperatorParameters.getImagePullPolicy());
        parameters.setJobManagerMemory(seatunnelFlinkOperatorParameters.getJobManagerMemory());
        parameters.setTaskManagerMemory(seatunnelFlinkOperatorParameters.getTaskManagerMemory());
        parameters.setSlot(seatunnelFlinkOperatorParameters.getSlot());
        parameters.setTaskManager(seatunnelFlinkOperatorParameters.getTaskManager());
        parameters.setParallelism(seatunnelFlinkOperatorParameters.getParallelism());
        parameters.setParamsMap(ParameterUtils.convert(paramsMap));

        // 配置的json内容
        parameters.setRawScript(seatunnelFlinkOperatorParameters.getRawScript());

        return JSONUtils.toJsonString(parameters);
    }

}
