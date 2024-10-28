package org.apache.dolphinscheduler.api.utils;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class WordVO implements Serializable {
    private String type;
    /**
     * 租户id
     */
    private int tenantCode;

    /**
     * 用户名称
     */
    private  String userName;

    private List<ModelVO> modelList;

    private TpDatasetVO tpDatasetVO;
}
