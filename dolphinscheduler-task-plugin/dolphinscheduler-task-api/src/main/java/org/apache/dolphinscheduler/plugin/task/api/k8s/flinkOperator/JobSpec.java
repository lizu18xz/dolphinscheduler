/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.builder.DiffResult;
import org.apache.commons.lang3.builder.Diffable;
import org.apache.commons.lang3.builder.ReflectionDiffBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Flink job spec.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
public class JobSpec implements Diffable<JobSpec> {

    /**
     * Optional URI of the job jar within the Flink docker container. For example:
     * local:///opt/flink/examples/streaming/StateMachineExample.jar. If not specified the job jar
     * should be available in the system classpath.
     */
    private String jarURI;

    /**
     * Parallelism of the Flink job.
     */
    private int parallelism = 1;

    /**
     * Fully qualified main class name of the Flink job.
     */
    private String entryClass;

    /**
     * Arguments for the Flink job main class.
     */
    private String[] args = new String[0];

    /**
     * Desired state for the job.
     */
    private JobState state = JobState.RUNNING;

    /**
     * Nonce used to manually trigger savepoint for the running job. In order to trigger a
     * savepoint, change the number to anything other than the current value.
     */
    private Long savepointTriggerNonce;

    /**
     * Savepoint path used by the job the first time it is deployed. Upgrades/redeployments will not
     * be affected.
     */
    private String initialSavepointPath;

    /**
     * Upgrade mode of the Flink job.
     */
    private UpgradeMode upgradeMode = UpgradeMode.STATELESS;

    /**
     * Allow checkpoint state that cannot be mapped to any job vertex in tasks.
     */
    private Boolean allowNonRestoredState;

    @Override
    public DiffResult<JobSpec> diff(JobSpec right) {
        ReflectionDiffBuilder builder = new ReflectionDiffBuilder(this, right, ToStringStyle.DEFAULT_STYLE);
        return builder.build();
    }
}
