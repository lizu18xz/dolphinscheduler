package org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob;

import lombok.Data;

@Data
public class Policies {

    private String event;

    private String action;


}
