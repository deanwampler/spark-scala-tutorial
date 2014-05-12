package spark.util

/** 
 * A case class used for the exercises involving SQL APIs. It represents a
 * "record" for a a verse from the King James Version of the Bible, as well as
 * the verses in some of the other sacred texts in the data directory.
 * Recall that each line has the format:
 *   <pre><code>book|chapter|verse| text.~</code></pre>
 * We use a case class to define the schema, as required by Spark SQL.
 */
case class Verse(book: String, chapter: Int, verse: Int, text: String)
