package org.apache.dolphinscheduler.plugin.task.api.k8s.spark;

/**
 * @author lizu
 * @since 2022/5/1
 */
public class RestartPolicy {

    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
