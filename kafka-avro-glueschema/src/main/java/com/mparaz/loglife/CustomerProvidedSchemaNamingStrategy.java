package com.mparaz.loglife;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import org.apache.avro.Schema;

public class CustomerProvidedSchemaNamingStrategy implements AWSSchemaNamingStrategy {
    @Override
    public String getSchemaName(String p0) {
        return p0;
    }

    @Override
    public String getSchemaName(String transportName,
                                Object data) {
        Schema schema = AVROUtils.getInstance()
                .getSchema(data);
        return String.format("%s-%s", transportName, schema.getName());
    }

    @Override
    public String getSchemaName(String transportName,
                                Object data,
                                boolean isKey) {
        Schema schema = AVROUtils.getInstance()
                .getSchema(data);
        return String.format("%s-%s-%s", transportName, schema.getName(), isKey ? "key" : "value");
    }
}