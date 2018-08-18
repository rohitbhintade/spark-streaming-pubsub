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
import java.util

import com.google.api.gax.core.{FixedCredentialsProvider, InstantiatingExecutorProvider}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1.{AcknowledgeRequest, ProjectSubscriptionName, PullRequest}

import scala.collection.JavaConversions._

object SubscriberSync {

  def main(args: Array[String]): Unit = {

    val ackIds = new util.ArrayList[String]

    //PubSub related configs
    val project = "bigdata-sandbox"
    val subscription = "pubsubtest_sub"
    val keyFilePath = "/Users/focus/projects/spark-streaming-pubsub/src/main/resources/bigdata-sandbox-be3753896350.json"
    val credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(keyFilePath)))

    val subscriberStubSettings = SubscriberStubSettings.newBuilder.setCredentialsProvider(credentialsProvider).build()

    val subscriber = GrpcSubscriberStub.create(subscriberStubSettings)

    val subscriptionNameString = ProjectSubscriptionName.format(project, subscription)
    val pullRequest = PullRequest.newBuilder.
      setMaxMessages(100).
      setReturnImmediately(false).
      setSubscription(subscriptionNameString).
      build

    // use pullCallable().futureCall to asynchronously perform this operation
    val pullResponse = subscriber.pullCallable().call(pullRequest)

    // handle received message
    for (message <- pullResponse.getReceivedMessagesList) {
      println(message.getMessage.getData.toStringUtf8)
      ackIds.add(message.getAckId)
    }

    val acknowledgeRequest = AcknowledgeRequest.newBuilder.setSubscription(subscriptionNameString).addAllAckIds(ackIds).build
    // use acknowledgeCallable().futureCall to asynchronously perform this operation
    subscriber.acknowledgeCallable.call(acknowledgeRequest)

    println(s"Number of pulled messages - ${pullResponse.getReceivedMessagesList.length}" )

    if(subscriber != null) subscriber.close()
  }

}
