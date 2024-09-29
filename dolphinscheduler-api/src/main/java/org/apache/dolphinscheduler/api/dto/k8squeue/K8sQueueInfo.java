package org.apache.dolphinscheduler.api.dto.k8squeue;

import io.fabric8.kubernetes.client.utils.Serialization;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;


@Data
public class K8sQueueInfo {

    private String apiVersion;

    private String kind;

    private Metadata metadata;

    private Spec spec;

    public static void main(String[] args) {
        K8sQueueInfo queueInfo = new K8sQueueInfo();
        queueInfo.setApiVersion("scheduling.volcano.sh/v1beta1");
        K8sQueueInfo.Metadata metadata = new K8sQueueInfo.Metadata();
        metadata.setName("default");
        queueInfo.setMetadata(metadata);
        Spec spec = new Spec();
        spec.setReclaimable(false);
        Map<String, Object> deserved = new HashMap<>();
        deserved.put("cpu", 1);
        deserved.put("memeory", "8Gi");
        spec.setDeserved(deserved);
        queueInfo.setSpec(spec);
        System.out.println(Serialization.asYaml(queueInfo));
    }
    @Data
    public static class Metadata {
        private String name;
    }

    @Data
    public static  class Spec {
        private Boolean reclaimable;

        private Map<String, Object> deserved;
    }
}

