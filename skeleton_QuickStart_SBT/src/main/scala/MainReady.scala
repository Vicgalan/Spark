import org.apache.log4j._

object MainReady {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print error
    Logger.getLogger("org").setLevel(Level.ERROR)

    def runsomething() = new doSomething().youWant()
    runsomething()

  }

}
