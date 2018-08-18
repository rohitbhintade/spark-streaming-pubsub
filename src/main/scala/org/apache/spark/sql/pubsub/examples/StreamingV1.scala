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

package org.apache.spark.sql.pubsub.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}

object StreamingV1 extends App {


  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("PubSubStreamingExample")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/test_121ad13123")
    .getOrCreate()

  //sparkSession.sparkContext.setLogLevel("DEBUG")

  val keyFilePath = "/Users/focus/projects/spark-streaming-pubsub/src/main/resources/bigdata-sandbox-be3753896350.json"

  val reader = sparkSession
    .readStream
    .format("pubsubV1")
    .option("projectId", "bigdata-sandbox")
    .option("subscriptionId", "pubsubtest_sub")
    .option("maxMessages", "1")
    .option("jsonKey",keyFilePath)
    .load()

  val pubSub = reader
    .writeStream
    .format("console")
    .trigger(ProcessingTime(7000L))
    .queryName("examplePubSub")
    .start()

  pubSub.awaitTermination()

}