package org.scoula.domain.cloudwatch.dto;

import lombok.Data;

@Data
public class Common {
    private String callApiPath;
    private String apiMethod;
    private String srcIp;
    private String deviceInfo;
    private String memberId;
    private String ledgerCode;
    private String transactionGroupId;
    private Integer retryCount;
}
