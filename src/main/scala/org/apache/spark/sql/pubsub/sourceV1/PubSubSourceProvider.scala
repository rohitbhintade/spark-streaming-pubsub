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

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.pubsub.util.Logging

/**
 * The provider class for the [[PubSubStreamingSource]]. This provider is designed such that it throws
 * IllegalArgumentException when the PubSub Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[pubsub] class PubSubSourceProvider extends StreamSourceProvider with DataSourceRegister  with Logging {

  /**
    * Returns the name and schema of the source. In addition, it also verifies whether the options
    * are correct and sufficient to create the [[PubSubStreamingSource]] when the query is started.
    */
  override def sourceSchema(
                             sqlContext: SQLContext,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): (String, StructType) = {
    require(schema.isEmpty, "PubSub source has a fixed schema and cannot be set with a custom one")
    validateOptions(parameters)
    ("PubSub", PubSubStreamingSource.pubSubSchema)
  }

  override def createSource(
                             sqlContext: SQLContext,
                             metadataPath: String,
                             schema: Option[StructType],
                             providerName: String,
                             parameters: Map[String, String]): Source = {

    validateOptions(parameters)

    val projectId = parameters(PubSubSourceProvider.projectId)
    val subscriptionId = parameters(PubSubSourceProvider.subscriptionId)
    val jsonKey = parameters(PubSubSourceProvider.jsonKey)

    new PubSubStreamingSource(
      sqlContext,
      projectId,
      subscriptionId,
      jsonKey,
      parameters,
      metadataPath)
  }

  private def validateOptions(parameters: Map[String, String]): Unit = {

    //validating projectId
      parameters.get(PubSubSourceProvider.projectId) match {
        case Some(value) => value
        case None => throw new IllegalArgumentException(s"ProjectId should be set !!")
      }

    //validating subscriptionId
      parameters.get(PubSubSourceProvider.subscriptionId) match {
        case Some(value) => value
        case None => throw new IllegalArgumentException(s"SubscriptionId should be set !!")
      }

    //validating jsonKey
      parameters.get(PubSubSourceProvider.jsonKey) match {
        case Some(value) => value
        case None => throw new IllegalArgumentException(s"jsonKey should be set !!")
        }

  }

  override def shortName(): String = "pubsubV1"

}

private[pubsub] object PubSubSourceProvider {
  val projectId = "projectId"
  val subscriptionId = "subscriptionId"
  val maxMessages = "maxMessages"
  val jsonKey = "jsonKey"
}