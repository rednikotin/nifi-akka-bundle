package pythian.nifi.processors

import com.google.gson.Gson
import scala.util.{ Success, Try }
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object LineProcessor {
  self =>
  val GSON = new Gson()
  type Processor = String => (String, String)
  private val processors = new java.util.concurrent.ConcurrentHashMap[String, Processor]()
  private val toolbox = currentMirror.mkToolBox()
  def compile(code: String): Try[Processor] = {
    Option(processors.get(code)) match {
      case Some(compiled) => Success(compiled)
      case None => self.synchronized {
        Option(processors.get(code)) match {
          case Some(compiled) => Success(compiled)
          case None =>
            Try {
              val wrapped = s"val fun: String => (String, String) = {\n$code\n}\nfun"
              val tree = toolbox.parse(wrapped)
              val compiled = toolbox.eval(tree).asInstanceOf[Processor]
              processors.put(code, compiled)
              compiled
            }
        }
      }
    }
  }
}