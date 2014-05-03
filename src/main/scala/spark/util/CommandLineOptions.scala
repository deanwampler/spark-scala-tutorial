package spark.util

import CommandLineOptions.{Opt, NameValue, Parser}

/** 
 * Handles command-line argument processing for scripts that take
 * help, input, and output arguments.
 */
case class CommandLineOptions(programName: String, opts: Opt*) {

  // Help message
  def helpMsg = s"""
    |usage: java ... $programName [options] 
    |where the options are the following:
    |  -h | --help  Show this message and quit.
    |""".stripMargin + opts.map(_.help).mkString("\n")

  lazy val matchers: Parser = 
    (opts foldLeft help) { 
      (partialfunc, opt) => partialfunc orElse opt.parser 
    } orElse noMatch
  
  protected def processOpts(args: Seq[String]): Seq[NameValue] =
    args match {
      case Nil => Nil
      case _ => 
        val (newArg, rest) = matchers(args)
        newArg +: processOpts(rest)
    }

  def apply(args: Seq[String]): Map[String,String] = {
    val foundOpts = processOpts(args)
    // Construct a map of the default args, then override with the actuals
    val map1 = opts.map(opt => opt.name -> opt.value).toMap
    // The actuals are already key-value pairs:
    val map2 = foundOpts.toMap
    val finalMap = map1 ++ map2
    println(s"$programName:")
    finalMap foreach {
      case (key, value) => printf("  %15s: %s\n", key, value)
    }
    finalMap
  }

  /** 
   * Common argument: help 
   * Use T = Any for convenient typing when we string these together.
   */
  val help: Parser = {
    case ("-h" | "--h" | "--help") +: tail => quit("", 0)
  }

  /** No match! */
  val noMatch: Parser = {
    case head +: tail =>
      quit(s"Unrecognized argument (or missing second argument): $head", 1)
  }

  def quit(message: String, status: Int): Nothing = { 
    if (message.length > 0) println(message)
    println(helpMsg)
    sys.exit(status)
  }
}

object CommandLineOptions {

  type NameValue = (String, String)
  type Parser = PartialFunction[Seq[String], (NameValue, Seq[String])]

  case class Opt(
    /** Used as a map key in the returned options. */
    name:  String,
    /** Initial value is the default; new option instance has the actual. */
    value: String,
    /** Help string displayed if user asks for help. */
    help:  String,
    /** Attempt to parse input words. If successful, return new value, rest of args. */
    parser: Parser) 

  /** Common argument: The input path */
  def inputPath(value: String): Opt = Opt(
    name   = "input-path",
    value  = value,
    help   = "-i | --in  | --input  path   The input root directory of files to crawl (default: $value)",
    parser = {
      case ("-i" | "--in" | "--inpath") +: path +: tail => (("input-path", path), tail)
    })

  /** Common argument: The output path */
  def outputPath(value: String): Opt = Opt(
    name   = "output-path",
    value  = value,
    help   = "-o | --out | --output path   The output location (default: $value)",
    parser = {
      case ("-o" | "--out" | "--outpath") +: path +: tail => (("output-path", path), tail)
    })

  /** Common argument: The Spark "master" */
  def master(value: String): Opt = Opt(
    name   = "master",
    value  = value,
    help   = """
       |-m | --master M      The "master" argument passed to SparkContext, "M" is one of:
       |                    "local", local[N]", "mesos://host:port", or "spark://host:port"
       |                    (default: $defaultMaster).""".stripMargin,
    parser = {
      case ("-m" | "--master") +: master +: tail => (("master", master), tail)
    })
}
