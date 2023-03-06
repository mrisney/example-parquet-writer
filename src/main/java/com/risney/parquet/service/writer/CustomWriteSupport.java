package com.risney.parquet.service.writer;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class CustomWriteSupport extends WriteSupport<List<String>> {

    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> columnDescriptors;

    CustomWriteSupport(MessageType schema) {
        this.schema = schema;
        this.columnDescriptors = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new HashMap<String, String>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(List<String> column) {

        if(column.size() != columnDescriptors.size())
            throw new ParquetEncodingException("Invalid input data. Expecting " +
                columnDescriptors.size() + " columns. Input had " + column.size() +
                    " columns (" + columnDescriptors + ") : " + column);

        recordConsumer.startMessage();
        int columns = columnDescriptors.size();

        for (int i = 0; i < columns; i++) {
            String val = column.get(i);
            if(val.length() > 0){
                recordConsumer.startField(columnDescriptors.get(i).getPath()[0], i);
                switch(columnDescriptors.get(i).getType()){
                    case BOOLEAN:
                        recordConsumer.addBoolean(Boolean.parseBoolean(val));
                        break;
                    case FLOAT:
                        recordConsumer.addFloat(Float.parseFloat(val));
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble(Double.parseDouble(val));
                        break;
                    case INT32:
                        recordConsumer.addInteger(Integer.parseInt(val));
                        break;
                    case INT64:
                        recordConsumer.addLong(Long.parseLong(val));
                        break;
                    case BINARY:
                        recordConsumer.addBinary(stringToBinary(val));
                        break;
                    default:
                        throw new ParquetEncodingException("Unsupported column type: " +
                                columnDescriptors.get(i).getType());
                }
                recordConsumer.endField(columnDescriptors.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }

    private Binary stringToBinary(Object value){
        return Binary.fromString(value.toString());
    }
}