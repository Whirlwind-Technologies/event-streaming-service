package com.nnipa.eventstreaming.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor; /**
 * Schema Information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaInfo {
    private String subject;
    private int version;
    private int id;
    private String schema;
    private String schemaType;
    private String compatibility;
}
