/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package io.confluent.connect.hdfs.parquet;

import io.confluent.connect.hdfs.HdfsSinkConnector;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.codec.CompressionCodecNotSupportedException;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.RecordWriter;

public class ParquetRecordWriterProvider implements RecordWriterProvider {

  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);

  private final static String EXTENSION = ".parquet";
  private HdfsSinkConnectorConfig connectorConfig;

  public ParquetRecordWriterProvider(HdfsSinkConnectorConfig connectorConfig) {
    this.connectorConfig = connectorConfig;
  }

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter<SinkRecord> getRecordWriter(
      Configuration conf, final String fileName, SinkRecord record, final AvroData avroData)
      throws IOException {
    final Schema avroSchema = avroData.fromConnectSchema(record.valueSchema());
    final CompressionCodecName compressionCodecName = getCompressionCodecName();

    int blockSize = 256 * 1024 * 1024;
    int pageSize = 64 * 1024;

    Path path = new Path(fileName);
    final ParquetWriter<GenericRecord> writer =
        new AvroParquetWriter<>(path, avroSchema, compressionCodecName, blockSize, pageSize, true, conf);

    return new RecordWriter<SinkRecord>() {
      @Override
      public void write(SinkRecord record) throws IOException {
        Object value = avroData.fromConnectData(record.valueSchema(), record.value());
        writer.write((GenericRecord) value);
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    };
  }

  private CompressionCodecName getCompressionCodecName() {
    String compressionCodec = connectorConfig.getString(HdfsSinkConnectorConfig.FORMAT_CLASS_COMPRESSION_CONFIG);
    switch(compressionCodec) {
        case "uncompressed": return CompressionCodecName.UNCOMPRESSED;
        case "snappy": return CompressionCodecName.SNAPPY;
        case "gzip": return CompressionCodecName.GZIP;
        case "lzo": return CompressionCodecName.LZO;
        default: return CompressionCodecName.SNAPPY;
    }
  }
}
