
package org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob;

import lombok.Data;

import java.util.List;

@Data
public class QueueJobSpec {

    private Integer minAvailable;

    private String schedulerName;

    private List<Plugins> plugins;

    private String queue;

    private List<Policies> policies;

    private List<Tasks> tasks;


}
