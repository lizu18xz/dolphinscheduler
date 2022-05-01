package org.apache.dolphinscheduler.plugin.task.api.k8s.spark;

/**
 * @author lizu
 * @since 2022/5/1
 */
public class Volume {

    private String name;

    private HostPath hostPath;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HostPath getHostPath() {
        return hostPath;
    }

    public void setHostPath(HostPath hostPath) {
        this.hostPath = hostPath;
    }

    public static class HostPath {

        private String path;

        private String type;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

}
