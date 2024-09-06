package org.apache.dolphinscheduler.api.service;

import org.apache.dolphinscheduler.api.dto.external.ImageRequest;
import org.apache.dolphinscheduler.api.dto.external.ImageResponse;

import java.util.List;

public interface ExternalSysService {

    List<ImageResponse> imageList(ImageRequest imageRequest);

}
