package org.apache.dolphinscheduler.api.dto.processTemplate;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessTemplateRequest {
    private Integer id;

    /**
     * code
     */
    private long code;

    private String projectName;

    /**
     * name
     */
    private String name;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;

}