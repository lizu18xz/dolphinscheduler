package org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob;

import lombok.Data;

import java.util.List;

@Data
public class Plugins {

    private List<String> pytorch;

}
