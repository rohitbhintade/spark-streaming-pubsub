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

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset => OffsetV2}

case class LongOffsetV2(value: Long) extends OffsetV2 {

  def apply(offset: SerializedOffset): LongOffsetV2 = {
    new LongOffsetV2(offset.json.toLong)
  }

  override def json(): String = value.toString

  def +(increment: Long): LongOffsetV2 = new LongOffsetV2(value + increment)

  def -(decrement: Long): LongOffsetV2 = new LongOffsetV2(value - decrement)
}

object LongOffsetV2 {


  def apply(offset: SerializedOffset): LongOffsetV2 = new LongOffsetV2(offset.json.toLong)

  /**
    * Convert generic Offset to LongOffset if possible.
    *
    * @return converted LongOffset
    */
  def convert(offset: Offset): Option[LongOffsetV2] = offset match {
    case lo: LongOffsetV2 => Some(lo)
    case so: SerializedOffset => Some(LongOffsetV2(so))
    case _ => None
  }

}
