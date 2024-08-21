package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import lombok.Data;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.CrdConstants;

@Group("kubeflow.org")
@Version("v1")
@Data
public class PyTorchJob extends CustomResource<PyTorchJobSpec, PytorchStatus> {


}
