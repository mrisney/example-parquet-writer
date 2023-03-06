package com.risney.parquet.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.risney.parquet.model.User;
import com.risney.parquet.service.writer.CustomParquetWriter;

@Service
public class ParquetService {

    @Value("${schema.filePath}")
    protected String schemaFilePath;

    @Value("${output.directoryPath}")
    protected String directoryPath;

    protected Path getUserPath() {
        String outputPath = directoryPath + "/users.parquet";
        File outputFile = new File(outputPath);
        Path path = new Path(outputFile.toURI().toString());
        return path;
    }

    protected CustomParquetWriter getParquetWriter(MessageType schema) throws Exception {
        return new CustomParquetWriter(getUserPath(), schema, false, CompressionCodecName.SNAPPY);
    }

    protected MessageType getUserSchema(String schemaFilePath) throws IOException {
        File resource = new File(schemaFilePath);
        String rawSchema = new String(Files.readAllBytes(resource.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    protected List<List<String>> getUserData(List<User> userList) {
        List<List<String>> userData = new ArrayList<>();
        for (User user : userList) {
            List<String> item = new ArrayList<>();
            item.add(user.getId().toString());
            item.add(user.getUsername());
            item.add(String.valueOf(user.isActive()));
            userData.add(item);
        }
        return userData;
    }
}