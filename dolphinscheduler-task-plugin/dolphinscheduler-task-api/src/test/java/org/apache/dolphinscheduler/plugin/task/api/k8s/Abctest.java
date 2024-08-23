package org.apache.dolphinscheduler.plugin.task.api.k8s;

import java.io.IOException;

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

    }

}
