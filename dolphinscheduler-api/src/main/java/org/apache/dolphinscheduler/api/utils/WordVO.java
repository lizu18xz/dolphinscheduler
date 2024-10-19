package org.apache.dolphinscheduler.api.utils;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class WordVO implements Serializable {
    private String type;

    private List<ModelVO> modelList;

    private TpDatasetVO tpDatasetVO;
}
