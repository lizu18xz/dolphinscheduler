package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class OutPutVolumeResponse {

    private String outputVolumeId;

    private String outputVolumeName;

    private String outputVolumeNameInfo;

    private List<OutPutVolumeResponse> children;

}
