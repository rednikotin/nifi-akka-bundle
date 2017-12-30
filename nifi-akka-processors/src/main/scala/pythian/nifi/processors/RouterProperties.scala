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

import java.io.{ PrintWriter, StringWriter }
import org.apache.nifi.components.{ PropertyDescriptor, ValidationContext, ValidationResult, Validator }
import scala.util.Failure

trait RouterProperties {
  val PROCESSOR: PropertyDescriptor =
    new PropertyDescriptor.Builder()
      .name("Processor code")
      .description("Function String => (String, String)")
      .required(true)
      .addValidator(RouterProperties.CODE_VALIDATOR)
      .build

  lazy val properties = List(PROCESSOR)
}

object RouterProperties extends RouterProperties {
  private val CODE_VALIDATOR = new Validator() {
    override def validate(subject: String, input: String, context: ValidationContext): ValidationResult = {
      val compiled = LineProcessor.compile(input)
      val text = compiled match {
        case Failure(ex) =>
          val sw = new StringWriter()
          val pw = new PrintWriter(sw)
          ex.printStackTrace(pw)
          sw.toString
        case _ =>
      }
      new ValidationResult.Builder()
        .subject(subject)
        .input(input)
        .explanation(s"Compilation failed with exception\n" + text)
        .valid(compiled.isSuccess)
        .build()
    }
  }
}
