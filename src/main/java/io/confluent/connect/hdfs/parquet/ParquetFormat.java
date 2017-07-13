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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.Format;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.RecordWriterProvider;
import io.confluent.connect.hdfs.SchemaFileReader;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;

public class ParquetFormat implements Format {

  private HdfsSinkConnectorConfig connectorConfig;

  public ParquetFormat(HdfsSinkConnectorConfig connectorConfig) {
    this.connectorConfig = connectorConfig;
  }

  @Override
  public RecordWriterProvider getRecordWriterProvider() {
    return new ParquetRecordWriterProvider(connectorConfig);
  }

  @Override
  public SchemaFileReader getSchemaFileReader(AvroData avroData) {
    return new ParquetFileReader(avroData);
  }

  @Override
  public HiveUtil getHiveUtil(HdfsSinkConnectorConfig config, AvroData avroData, HiveMetaStore hiveMetaStore) {
    return new ParquetHiveUtil(config, avroData, hiveMetaStore);
  }
}
