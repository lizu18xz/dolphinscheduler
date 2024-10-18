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

    private String id;

    private String name;

    private String outputInfo;

    private List<OutPutVolumeResponse> children;

}
