package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.external.*;

import java.util.List;

public interface ExternalSysService {

    List<ImageResponse> imageList(ImageRequest imageRequest);

    List<WrapFetchVolumeResponse> fetchVolumeList(FetchVolumeRequest request);


    List<StorageResponse> storagePage(StorageRequest request);

    List<OutPutVolumeResponse> getVolumeOutput(StorageRequest request);
}
