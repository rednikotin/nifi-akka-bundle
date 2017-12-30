/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pythian.nifi.processors

import java.io._
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import akka.actor.ActorSystem
import scala.util.Try
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.components.PropertyDescriptor
import org.apache.nifi.processor.{ AbstractProcessor, Relationship }
import org.apache.nifi.processor.{ ProcessContext, ProcessSession }
import org.apache.nifi.annotation.documentation.Tags
import org.apache.nifi.processor.util.StandardValidators

@Tags(Array("Router"))
class Router extends AbstractProcessor with RouterProperties with RouterRelationships {

  import scala.collection.JavaConverters._
  import LineProcessor._

  private val dynamicRelationships: AtomicReference[Map[String, Relationship]] = new AtomicReference(Map.empty)

  override def getSupportedPropertyDescriptors: java.util.List[PropertyDescriptor] = {
    properties.asJava
  }

  override def getRelationships: java.util.Set[Relationship] = {
    (relationships ++ dynamicRelationships.get.values).asJava
  }

  private val processorId = "ProcessorSystem-" + UUID.randomUUID().toString
  implicit val system: ActorSystem = ActorSystem(processorId)

  sealed trait MessageType { def name: String }
  object ErrorMessageType extends MessageType { val name = "processor.errors" }
  case class DataMessageType(name: String) extends MessageType

  sealed trait Message {
    def messageType: MessageType
    def data: String
  }
  case class GoodMessage(tpe: String, data: String) extends Message {
    val messageType = DataMessageType(tpe)
  }
  case class BadMessage(data: String) extends Message {
    val messageType: MessageType = ErrorMessageType
  }

  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {
    if (propertyDescriptorName == RelUnmatched.getName) {
      null
    } else {
      new PropertyDescriptor.Builder()
        .required(false)
        .name(propertyDescriptorName)
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .dynamic(true)
        .build()
    }
  }

  override def onPropertyModified(descriptor: PropertyDescriptor, oldValue: String, newValue: String): Unit = {
    if (descriptor.isDynamic) {
      val cur = dynamicRelationships.get()
      val relationship = new Relationship.Builder()
        .name(descriptor.getName)
        .description(s"Routing based on key=${descriptor.getName}")
        .build()
      val next = if (newValue == null || !newValue.toBoolean) {
        cur - relationship.getName
      } else {
        cur + (relationship.getName -> relationship)
      }
      this.dynamicRelationships.set(next)
    }
  }

  override def onTrigger(context: ProcessContext, session: ProcessSession): Unit = {
    val inFlowFile = session.get

    Option(inFlowFile) match {
      case Some(_) =>
        val processorCode = context.getProperty(PROCESSOR).getValue
        val processor = compile(processorCode).get

        val filename = inFlowFile.getAttribute("filename")
        val in = session.read(inFlowFile)

        case class FlowFileSink(s: MessageType) extends RoutingSink.Sink[Message, FlowFile] {
          private val outFileName = filename + "." + s.name
          //Paths.get(outFileName)
          private val outFlowFile = session.create(inFlowFile)
          session.putAttribute(outFlowFile, "filename", outFileName)
          private val out = session.write(outFlowFile)
          private var nonEmpty = false
          private val writer = new BufferedWriter(new OutputStreamWriter(out))
          def add(elem: Message): Unit = {
            if (nonEmpty) writer.newLine()
            nonEmpty = true
            writer.write(elem.data)
          }
          def close(): FlowFile = {
            writer.flush()
            writer.close()
            outFlowFile
          }
        }

        val groupBy = RoutingSink.create[Message, MessageType, FlowFile](FlowFileSink, _.messageType)

        val br = new BufferedReader(new InputStreamReader(in))
        var line: String = null
        while ({
          line = br.readLine()
          line != null
        }) {
          val data = Try {
            val (tpe, data) = processor(line)
            GoodMessage(tpe, data)
          }.getOrElse(BadMessage(line))
          groupBy.add(data)
        }
        in.close()

        val files = groupBy.close()

        files.foreach {
          case (ErrorMessageType, file) =>
            session.transfer(file, RelIncompatible)
          case (DataMessageType(name), file) =>
            val rel = dynamicRelationships.get.getOrElse(name, RelUnmatched)
            session.transfer(file, rel)
        }

        session.remove(inFlowFile)

      case None =>

    }

  }
}
