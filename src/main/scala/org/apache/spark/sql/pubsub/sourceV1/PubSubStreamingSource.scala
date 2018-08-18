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

package org.apache.spark.sql.pubsub.sourceV1

import java.io._

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1.{AcknowledgeRequest, ProjectSubscriptionName, PullRequest}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{LongOffset, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.pubsub.util.Logging

import scala.collection.JavaConversions._
import scala.collection.mutable

private[pubsub] class PubSubStreamingSource(
    sqlContext: SQLContext,
    projectId: String,
    subscriptionId: String,
    jsonKey : String,
    sourceOptions: Map[String, String],
    metadataPath: String)
  extends Source with Logging {

  import PubSubStreamingSource._

  override def schema: StructType = pubSubSchema

  def checkpointFile(name: String): String = new Path(new Path(baseCheckPointDirPath), name).toUri.toString
  val baseCheckPointDirPath: String = metadataPath.substring(0, metadataPath.indexOf("/sources/"))
  val offsetLog = new OffsetSeqLog(sqlContext.sparkSession, checkpointFile("offsets"))
  val maxMessages: Int = sourceOptions.getOrElse(PubSubSourceProvider.maxMessages, defaultMaxMessages.toString).toInt

  //Initial batchId
  var currentBatchId : Long = offsetLog.getLatest.map(_._1).getOrElse(1)

  //Mutable buffer to hold the acknowledgements of messages of current batch
  var ackBuffer = mutable.Buffer[String]()

  //Creating a subscriber connection instance for polling messages
  val subscriptionNameString = ProjectSubscriptionName.format(projectId, subscriptionId)
  val credentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKey)))
  val subscriberStubSettings = SubscriberStubSettings.newBuilder.setCredentialsProvider(credentialsProvider).build()

  var subscriber = GrpcSubscriberStub.create(subscriberStubSettings)

  /**
  * Returns the [DataFrame] of messages fetched from PubSub, the number messages fetched is equal to the [maxMessages]
  *
  */
  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {

    ackBuffer = mutable.Buffer[String]()

    val pullRequest = PullRequest.newBuilder.
      setMaxMessages(maxMessages).
      setReturnImmediately(false).
      setSubscription(subscriptionNameString).
      build

    // use pullCallable().futureCall to asynchronously perform this operation
    val pullResponse = subscriber.pullCallable().call(pullRequest)
    val messages = pullResponse.getReceivedMessagesList.map(_.getMessage).
      map(x => InternalRow(x.getMessageId,x.getData.toStringUtf8,x.getPublishTime.getSeconds)).toList

    ackBuffer = pullResponse.getReceivedMessagesList.map(_.getAckId)

    val messagesRDD = sqlContext.sparkContext.parallelize(messages)
    sqlContext.internalCreateDataFrame(messagesRDD,schema,isStreaming = true)
  }

  /** Returns the next available offset for this source. */
  override def getOffset: Option[Offset] = {
    currentBatchId = currentBatchId + 1
    Some(LongOffset(currentBatchId))
  }

  override def commit(end: Offset): Unit = {

    //Acknowledge all messages that has been successfully processed
    if(ackBuffer.nonEmpty) {
      val acknowledgeRequest = AcknowledgeRequest.newBuilder.setSubscription(subscriptionNameString).addAllAckIds(ackBuffer).build
      subscriber.acknowledgeCallable.call(acknowledgeRequest)
    }
  }
  /** Stop this source and free any resources it has allocated. */
  override def stop(): Unit = {
    subscriber.close()
  }
  override def toString(): String = s"PubSubSource - projects/$projectId/subscriptions/$subscriptionId"

}

/** Companion object for the [[PubSubStreamingSource]]. */
private[pubsub] object PubSubStreamingSource {

  private[pubsub] val VERSION = 2
  private[pubsub] val defaultMaxMessages = 1000L

  def pubSubSchema: StructType = StructType(Seq(
    StructField("messageId", StringType),
    StructField("data", StringType),
    StructField("publishTime", LongType)
  ))

}