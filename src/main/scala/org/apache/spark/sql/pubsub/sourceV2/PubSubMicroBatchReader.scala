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

import java.io.FileInputStream
import java.util
import java.util.Optional

import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStubSettings}
import com.google.pubsub.v1.{AcknowledgeRequest, ProjectSubscriptionName, PullRequest}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.streaming.{MicroBatchReader, Offset}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.pubsub.util.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable

class PubSubMicroBatchReader(options: DataSourceOptions) extends MicroBatchReader with Serializable with Logging {

  import PubSubStreamingSource._

  val sourceOptions = options.asMap()

  private val projectId : String = options.get(PROJECT_ID.toLowerCase).get
  private val subscriptionId : String = options.get(SUBSCRIPTION_ID.toLowerCase).get
  private val jsonKey : String = options.get(JSON_KEY.toLowerCase).get
  private val maxMessages : Int = options.get(MAX_MESSAGES.toLowerCase).orElse(defaultMaxMessages).toInt

  private val numPartitions = options.get(NUM_PARTITIONS).orElse(defaultPartitions).toInt
  private val numberOfMessagesPerPartition = maxMessages / numPartitions

  //Mutable buffer to hold the acknowledgements of messages of current batch
  var ackBuffer: mutable.Buffer[String] = mutable.Buffer[String]()

  def addAcks(acks : List[String]) : Unit = synchronized {
    ackBuffer = ackBuffer.union(acks)
  }

  //Creating a subscriber connection instance for polling messages
  val subscriptionNameString: String = ProjectSubscriptionName.format(projectId, subscriptionId)

  var n= 0

  private var start: LongOffsetV2 = _
  private var end: LongOffsetV2 = _

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {

    this.start = start.orElse(LongOffsetV2(0)).asInstanceOf[LongOffsetV2]
    this.end = end.orElse(LongOffsetV2(this.start.value + 1)).asInstanceOf[LongOffsetV2]

  }

  override def getStartOffset: Offset = {
    if (start == null) throw new IllegalStateException("start offset not set")
    start
  }
  override def getEndOffset: Offset = {
    if (end == null) throw new IllegalStateException("end offset not set")
    end
  }

  override def deserializeOffset(json: String): Offset = new LongOffsetV2(json.toLong)

  override def commit(end: Offset): Unit = {

    val credentialsProvider: FixedCredentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKey)))
    val subscriberStubSettings: SubscriberStubSettings = SubscriberStubSettings.newBuilder.setCredentialsProvider(credentialsProvider).build()
    val subscriber: GrpcSubscriberStub = GrpcSubscriberStub.create(subscriberStubSettings)

    //Acknowledge all messages that has been successfully processed
    if(ackBuffer.nonEmpty) {
      val acknowledgeRequest = AcknowledgeRequest.newBuilder.setSubscription(subscriptionNameString).addAllAckIds(ackBuffer.asJava).build
      subscriber.acknowledgeCallable.call(acknowledgeRequest)
    }
    info(s"PubSub : Total ${ackBuffer.size} messages are acknowleged")
    ackBuffer.clear()
    subscriber.close()
  }

  override def stop(): Unit = { }


  override def readSchema(): StructType = {
    StructType(Seq(
      StructField("messageId", StringType),
      StructField("data", StringType),
      StructField("publishTime", LongType)
    ))
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

    val credentialsProvider: FixedCredentialsProvider = FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(jsonKey)))
    val subscriberStubSettings: SubscriberStubSettings = SubscriberStubSettings.newBuilder.setCredentialsProvider(credentialsProvider).build()
    val subscriber: GrpcSubscriberStub = GrpcSubscriberStub.create(subscriberStubSettings)

    val pullRequest = PullRequest.newBuilder.
      setMaxMessages(maxMessages).
      setReturnImmediately(false).
      setSubscription(subscriptionNameString).
      build

    // use pullCallable().futureCall to asynchronously perform this operation
    val pullResponse = subscriber.pullCallable().call(pullRequest)
    val messages = pullResponse.getReceivedMessagesList.asScala.map(_.getMessage).
      map(x => (x.getMessageId,x.getData.toStringUtf8,x.getPublishTime.getSeconds)).toList

    val messagesParts = messages.sliding(numberOfMessagesPerPartition,numberOfMessagesPerPartition).take(numPartitions-1)
    val messagesLastPart = List(messages.slice(numberOfMessagesPerPartition * (numPartitions-1), messages.size))
    val partAckBuffer = pullResponse.getReceivedMessagesList.asScala.map(_.getAckId).toList
    addAcks(partAckBuffer)

    subscriber.close()
    (messagesParts ++ messagesLastPart).map(messages => PubSubBatchTask(messages).asInstanceOf[DataReaderFactory[Row]]).toList.asJava
  }

  case class PubSubBatchTask(values: Seq[(String,String, Long)]) extends DataReaderFactory[Row] {
    override def createDataReader(): DataReader[Row] = new PubSubBatchReader(values)
  }

  class PubSubBatchReader (values: Seq[(String,String, Long)]) extends DataReader[Row] {
    private var currentIndex = -1

    override def next(): Boolean = {
      // Return true as long as the new index is in the seq.
      currentIndex += 1
      currentIndex < values.size
    }

    override def get(): Row = {
      Row(
        values(currentIndex)._1,
        values(currentIndex)._2,
        values(currentIndex)._3
      )
    }
    override def close(): Unit = {}
  }

}
