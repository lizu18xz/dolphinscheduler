package org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob;

import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Version;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Group("batch.volcano.sh")
@Version("v1alpha1")
@Kind("Job")
@Data
public class QueueJob extends CustomResource<QueueJobSpec, QueueJobStatus> {






}
