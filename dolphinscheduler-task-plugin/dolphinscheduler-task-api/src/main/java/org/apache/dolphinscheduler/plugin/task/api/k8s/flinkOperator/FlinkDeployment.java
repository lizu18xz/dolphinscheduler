/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import lombok.Data;
import org.apache.dolphinscheduler.plugin.task.api.k8s.flinkOperator.status.FlinkDeploymentStatus;

@Group(CrdConstants.API_GROUP)
@Version(CrdConstants.API_VERSION)
@Data
public class FlinkDeployment extends
    AbstractFlinkResource<FlinkDeploymentSpec, FlinkDeploymentStatus>
    implements Namespaced {

    @Override
    public FlinkDeploymentStatus initStatus() {
        return new FlinkDeploymentStatus();
    }

    @Override
    public FlinkDeploymentSpec initSpec() {
        return new FlinkDeploymentSpec();
    }

}
