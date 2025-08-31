package com.nnipa.eventstreaming.model;

import com.nnipa.eventstreaming.enums.PartitionStrategy;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map; /**
 * Event Routing Rule
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventRoutingRule {
    private String ruleId;
    private String sourceTopic;
    private String targetTopic;
    private String condition;
    private PartitionStrategy partitionStrategy;
    private boolean active;
    private Map<String, String> transformations;
}
