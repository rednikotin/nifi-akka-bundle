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
import com.google.gson.Gson
import org.apache.nifi.processor.Relationship
import scala.util.Random
import org.scalatest._
import org.apache.nifi.util.{ TestRunner, TestRunners }

class RouterSpec extends FunSpec {
  import scala.collection.JavaConverters._
  import RouterProperties._
  import RouterRelationships._

  val someContent: String =
    """
      |{"type":"red", "payload":1}
      |{"type":"green", "payload":2}
      |{"type":"black", "payload":3}
      |{"type1":"black", "payload":4}
      |{"type":"black", "payload":5}
      |{"type":"green", "payload":6}
      |{"type":"black", "payload":7}
    """.stripMargin.trim

  val code: String =
    """import pythian.nifi.processors.LineProcessor.GSON
      |import com.google.gson.JsonObject
      |(line: String) => {
      |  val jo = GSON.fromJson(line, classOf[JsonObject])
      |  val tpe = jo.get("type").getAsString
      |  val payload = jo.get("payload")
      |  val data = GSON.toJson(payload)
      |  (tpe, data)
      |}""".stripMargin.trim

  val gson = new Gson()
  case class Payload(id: Int, data: Int)
  case class Row(`type`: String, payload: Payload)
  val bigContent: String =
    (1 to 100)
      .map(id => Row(Random.nextInt(3) match {
        case 0 => "red"
        case 1 => "green"
        case 2 => "blue"
      }, Payload(id, Random.nextInt(1000))))
      .map(r => gson.toJson(r))
      .mkString("\n")

  def printFiles(runner: TestRunner, rel: Relationship): Unit = {
    println(s"Relation: ${rel.getName}")
    for (flowFile <- runner.getFlowFilesForRelationship(rel).asScala) {
      val data = new String(runner.getContentAsByteArray(flowFile))
      println(s"file: ${flowFile.getAttribute("filename")}")
      println(data)
    }
  }

  describe("Router") {
    it("should successfully process a small FlowFile") {
      val processor = new Router
      val runner: TestRunner = TestRunners.newTestRunner(processor)
      runner.setProperty(PROCESSOR, code)
      runner.setProperty("black", "true")

      val content = new ByteArrayInputStream(someContent.getBytes)
      val relBlack = new Relationship.Builder().name("black").build()

      println(">>>" + code + "<<<")
      println(">>>" + someContent + "<<<")

      runner.enqueue(content)
      runner.run()

      runner.assertTransferCount(RelUnmatched, 2)
      runner.assertTransferCount(relBlack, 1)
      runner.assertTransferCount(RelIncompatible, 1)
      runner.assertTransferCount(RelFailure, 0)
    }
    it("should successfully process a big FlowFile") {
      val processor = new Router
      val runner: TestRunner = TestRunners.newTestRunner(processor)
      runner.setProperty(PROCESSOR, code)
      runner.setProperty("black", "true")
      runner.setProperty("red", "true")
      runner.setProperty("green", "false")

      val content = new ByteArrayInputStream(bigContent.getBytes)
      val relBlack = new Relationship.Builder().name("black").build()
      val relRed = new Relationship.Builder().name("red").build()
      val relGreen = new Relationship.Builder().name("green").build()

      runner.enqueue(content)
      runner.run()

      runner.assertTransferCount(RelUnmatched, 2)
      runner.assertTransferCount(relBlack, 0)
      runner.assertTransferCount(relRed, 1)
      runner.assertTransferCount(relGreen, 0)
      runner.assertTransferCount(RelIncompatible, 0)
      runner.assertTransferCount(RelFailure, 0)

      //printFiles(runner, relRed)
    }
  }
}
