package spark.activator.util

/** 
 * Handles command-line argument processing for scripts that take
 * help, input, and output arguments.
 */
case class CommandLineOptions(
  defaultInputPath:  String,
  defaultOutputPath: String,
  defaultMaster:     String,  
  programName:       String) {

  // Help message
  val help = s"""
  |usage: java ... $programName [-h|--help] \\ 
  |         [-i|--in|--input path] [-o|--out|--output path] [-m | --master M]
  |where:
  |  -h | --help   Show this message and quit.
  |  -i ... path   The input root directory of files to crawl (default: $defaultInputPath)
  |  -o ... path   The output location (default: $defaultOutputPath)
  |  -m ... M      The "master" argument passed to SparkContext, "M" is one of:
  |                  "local", local[N]", "mesos://host:port", or "spark://host:port"
  |                  (default: $defaultMaster). 
  |""".stripMargin

  def quit(message: String, status: Int): Nothing = { 
    if (message.length > 0) println(message)
    println(help)
    sys.exit(status)
  }

  // Hold the values to be used for input and output paths
  case class Args(
    inpath:  String = defaultInputPath,
    outpath: String = defaultOutputPath,
    master:  String = defaultMaster)
  
  // Process the command-line argument list
  def processArgs(argsList: List[String], argz: Args): Args = argsList match {
    case Nil => argz
    case ("--help" | "-h") :: tail =>
      quit("", 0)
    case ("--in" | "--inpath" | "-i") :: path :: tail =>
      processArgs(tail, argz copy (inpath = path))
    case ("--out" | "--outpath" | "-o") :: path :: tail =>
      processArgs(tail, argz copy (outpath = path))
    case ("--master" | "-m") :: m :: tail =>
      processArgs(tail, argz copy (master = m))
    case head :: tail =>
      quit(s"Unrecognized argument (or missing second argument): $head", 1)
  }

  def apply(argsList: List[String]): Args = {
    val args = processArgs(argsList, Args())
    println(s"$programName:")
    println(s"  Input:  ${args.inpath}")
    println(s"  Output: ${args.outpath}")
    println(s"  Master: ${args.master}")
    args
  }
}
