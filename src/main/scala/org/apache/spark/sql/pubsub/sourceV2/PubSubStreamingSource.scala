/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.pubsub.sourceV2

import java.util.Optional

import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, MicroBatchReadSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.pubsub.util.Logging

class PubSubStreamingSource extends DataSourceV2 with MicroBatchReadSupport with DataSourceRegister with Logging {

  override def createMicroBatchReader(schema: Optional[StructType],
                                      checkpointLocation: String,
                                      options: DataSourceOptions): MicroBatchReader = {
    new PubSubMicroBatchReader(options)
  }
  override def shortName(): String = "pubsub"
}


object PubSubStreamingSource {

  final val PROJECT_ID = "projectId"
  final val SUBSCRIPTION_ID = "subscriptionId"
  final val MAX_MESSAGES = "maxMessages"
  final val JSON_KEY = "jsonKey"
  final val NUM_PARTITIONS = "numPartitions"

  final val defaultMaxMessages = "1000"
  final val defaultPartitions = "1"
}