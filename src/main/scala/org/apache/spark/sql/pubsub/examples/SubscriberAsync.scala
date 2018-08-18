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
import java.util.concurrent.TimeUnit

import com.google.api.gax.core.{FixedCredentialsProvider, InstantiatingExecutorProvider}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.{AckReplyConsumer, MessageReceiver, Subscriber}
import com.google.pubsub.v1.{ProjectSubscriptionName, PubsubMessage, Subscription};

object SubscriberAsync {

  def main(args: Array[String]): Unit = {

    //PubSub related configs
    val project = "bigdata-sandbox"
    val subscription = "pubsubtest_sub"
    val keyFilePath = "/Users/focus/projects/spark-streaming-pubsub/src/main/resources/bigdata-sandbox-be3753896350.json"

    //subscriber related configs
    val subscriptionName = ProjectSubscriptionName.of(project, subscription)
    val credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(keyFilePath)))
    val executorProvider = InstantiatingExecutorProvider.newBuilder.setExecutorThreadCount(1).build

    //initializing subscriber
    val subscriber = Subscriber.newBuilder(subscriptionName, receiver).
      setCredentialsProvider(credentialsProvider).
      setExecutorProvider(executorProvider).
      build()

    subscriber.startAsync()

    subscriber.awaitTerminated(30,TimeUnit.SECONDS)
  }

  val receiver = new MessageReceiver() {
    override def receiveMessage(message: PubsubMessage, consumer: AckReplyConsumer): Unit = { // handle incoming message, then ack/nack the received message
      println("Data : " + message.getData.toStringUtf8)
      consumer.ack()
    }
  }
}
