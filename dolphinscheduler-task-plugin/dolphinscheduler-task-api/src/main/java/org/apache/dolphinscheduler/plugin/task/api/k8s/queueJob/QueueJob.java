package org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob;

import io.fabric8.kubernetes.client.CustomResource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class QueueJob extends CustomResource<QueueJobSpec, QueueJobStatus> {






}
