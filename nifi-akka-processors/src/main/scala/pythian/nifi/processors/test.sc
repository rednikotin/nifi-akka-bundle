import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

val code = """val fun: String => (String, String) = { (x: Any) => ("", "") } ; fun"""

val toolbox = currentMirror.mkToolBox()
val tree = toolbox.parse(code)


val compiled = toolbox.eval(tree).getClass

