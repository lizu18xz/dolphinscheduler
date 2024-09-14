package org.apache.dolphinscheduler.api.dto.external;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@Getter
@Setter
@AllArgsConstructor
public class StorageResponse {

    private String id;

    private String storageType;

    private String storageName;

    private String storageDesc;

    private String storageConfig;

    private String projectIds;

    private String projectName;

    private String rongqiIn;

    private String rongqiOut;

    private String suIn;

    private String suOut;

}
