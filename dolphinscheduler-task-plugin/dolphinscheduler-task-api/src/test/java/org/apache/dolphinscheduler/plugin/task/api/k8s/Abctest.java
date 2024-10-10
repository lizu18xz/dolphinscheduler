package org.apache.dolphinscheduler.plugin.task.api.k8s;

import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.PytorchK8sQueueTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.k8s.queueJob.QueueJob;

import java.io.IOException;
import java.io.InputStream;

public class Abctest {

    public static void main(String[] args) throws IOException {
        /*
         * InputStream resourceAsStream = null; resourceAsStream = PytorchK8sOperatorTaskExecutor.class
         * .getResourceAsStream("/pytorch-on-k8s-operator.yaml"); PyTorchJob pyTorchJob1= Serialization.yamlMapper()
         * .readValue(resourceAsStream, PyTorchJob.class);
         * 
         * System.out.println(pyTorchJob1.getMetadata());
         */
        System.out.println(" --firetime \"$(date -d \"$date\" +%s)\"");

        try {
            InputStream resourceAsStream = null;
            resourceAsStream = PytorchK8sQueueTaskExecutor.class
                    .getResourceAsStream("/pytorch-job.yaml");
            QueueJob job = Serialization.yamlMapper()
                    .readValue(resourceAsStream, QueueJob.class);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
