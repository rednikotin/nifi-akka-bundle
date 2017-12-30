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

  override def getSupportedDynamicPropertyDescriptor(propertyDescriptorName: String): PropertyDescriptor = {
    if (relationships.map(_.getName).contains(propertyDescriptorName)) {
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
        val filename = inFlowFile.getAttribute("filename")
        val in = session.read(inFlowFile)

        Try {
          val processorCode = context.getProperty(PROCESSOR).getValue
          val processor = compile(processorCode).get

          // message types
          trait KeyType { def name: String }
          object ErrorKeyType extends KeyType { val name: String = RelIncompatible.getName }
          case class DataKeyType(name: String) extends KeyType

          // messages
          trait KeyValue {
            def keyType: KeyType
            def value: String
          }
          case class GoodKeyValue(tpe: String, value: String) extends KeyValue { val keyType = DataKeyType(tpe) }
          case class ErrorKeyValue(value: String) extends KeyValue { val keyType: KeyType = ErrorKeyType }

          case class FlowFileSink(key: KeyType) extends RoutingSink.Sink[KeyValue, FlowFile] {
            private val outFileName = filename + "." + key.name
            //Paths.get(outFileName)
            private val outFlowFile = session.create(inFlowFile)
            session.putAttribute(outFlowFile, "filename", outFileName)
            private val out = session.write(outFlowFile)
            private var nonEmpty = false
            private val writer = new BufferedWriter(new OutputStreamWriter(out))

            def add(elem: KeyValue): Unit = {
              if (nonEmpty) writer.newLine()
              nonEmpty = true
              writer.write(elem.value)
            }

            def close(): FlowFile = {
              writer.flush()
              writer.close()
              outFlowFile
            }
          }

          val groupBy = RoutingSink.create[KeyValue, KeyType, FlowFile](FlowFileSink, _.keyType)

          val br = new BufferedReader(new InputStreamReader(in))
          var line: String = null
          while ( {
            line = br.readLine()
            line != null
          }) {
            val data = Try {
              val (tpe, data) = processor(line)
              GoodKeyValue(tpe, data)
            }.getOrElse(ErrorKeyValue(line))
            groupBy.add(data)
          }
          in.close()

          val files = groupBy.close()

          files.foreach {
            case (ErrorKeyType, file) =>
              session.transfer(file, RelIncompatible)
            case (DataKeyType(name), file) =>
              val rel = dynamicRelationships.get.getOrElse(name, RelUnmatched)
              session.transfer(file, rel)
          }

          session.remove(inFlowFile)
        } recover {
          case ex =>
            getLogger.error(s"Processing failed for file=$filename", ex)
            in.close()
            session.transfer(inFlowFile, RelFailure)
        }

      case None =>

    }

  }
}
