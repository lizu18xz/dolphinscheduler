
package org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator;

import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.status.CommonStatus;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;

public class AbstractFlinkResource<SPEC extends AbstractFlinkSpec, STATUS extends CommonStatus<SPEC>>
        extends
            CustomResource<SPEC, STATUS>
        implements
            Namespaced {
}
