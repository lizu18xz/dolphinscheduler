package org.apache.dolphinscheduler.plugin.task.api.k8s.pytorchOperator;

import lombok.Data;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("kubeflow.org")
@Version("v1")
@Data
public class PyTorchJob extends CustomResource<PyTorchJobSpec, PytorchStatus> {

}
