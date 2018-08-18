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

import java.io.FileInputStream

import com.google.api.core.ApiFuture
import com.google.pubsub.v1.ProjectTopicName
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import com.google.api.core.ApiFutures
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.ServiceAccountCredentials

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object PublisherAsync extends App {

  //PubSub related configs
  val project = "bigdata-sandbox"
  val topic = "pubsubtest"
  val keyFilePath = "/Users/focus/projects/spark-streaming-pubsub/src/main/resources/bigdata-sandbox-be3753896350.json"

  val credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(keyFilePath)))
  val topicName = ProjectTopicName.format(project, topic)
  val publisher = Publisher.newBuilder(topicName).setCredentialsProvider(credentialsProvider).build

  val messageIdFutures = ListBuffer[ApiFuture[String]]()

  for(number <- 1 to 10000) {

    val data = ByteString.copyFromUtf8(number.toString)
    val pubsubMessage = PubsubMessage.newBuilder.setData(data).build

    val messageIdFuture = publisher.publish(pubsubMessage)
    messageIdFutures.append(messageIdFuture)
  }

  val messageIds = ApiFutures.allAsList(messageIdFutures.asJava).get()

  println(s"\nPushed ${messageIds.size} messages successfully")

  if (publisher != null)
    publisher.shutdown()
}
