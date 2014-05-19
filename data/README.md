# README for the data directory

## Sacred Texts

The following ancient, sacred texts are from [www.sacred-texts.com/bib/osrc/](http://www.sacred-texts.com/bib/osrc/). All are copyright-free texts, where each verse is on a separate line, prefixed by the book name, chapter, number, and verse number, all "|" separated.

File | Description
:--- | :----------
`kjvdat.txt` | The King James Version of the Bible. For some reason, each line (one per verse) ends with a "~" character.
`t3utf.dat` | Tanach, the Hebrew Bible.
`vuldat.txt` | The Latin Vulgate.
`sept.txt` | The Septuagint (Koine Greek of the Hebrew Old Testament).
`ugntdat.txt` | The Greek New Testament.
`apodat.txt` | The Apocrypha (in English).
`abbrevs-to-names.tsv` | A map from the book abbreviations used in these texts to the full book names. Derived using data from the sacred-texts.com site.

There are many other texts from the world's religious traditions at the [www.sacred-texts.com](http://www.sacred-texts.com) site, but most of the others aren't formatted into one convenient file like these examples.

Here are Hive DDL statements for these files, if you want to put them into Hive.

For example, this DDL statement can be used for the `data/kjvdat.txt` file, where I'll assume you've copied the file to a directory `hdfs://server/data/kjvdat` in HDFS, which requires directory paths rather than file names, where `server` is the server name or IP address for the *NameNode*.

```
CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
  book    STRING,
  chapter INT,
  verse   INT,
  text    STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION 'hdfs://server/data/kjvdat';
```

Actually the `hdfs://server` prefix can be omitted. Hive will infer the correct file system type based on its configuration.

The same DDL can be used for the other files mentioned above, except for the name map file. `abbrevs-to-names.tsv`. Here is a DDL statement for the latter, assuming the file is copied to a directory `hdfs://server/data/abbrevs_to_names` in HDFS. 

```
CREATE EXTERNAL TABLE IF NOT EXISTS abbrevs_to_names (
  abbrev  STRING,
  book    STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://server/data/abbrevs_to_names';
```

Note that the field delimiter is tab, not "|".

## Email Classified as SPAM and HAM

A sample of SPAM/HAM classified emails from the well-known Enron email data set was adapted from [this research project](http://www.aueb.gr/users/ion/data/enron-spam/). Each file is plain text, partially formatted (i.e., with `name:value` headers) as used in email servers and clients.

Directory | Description
:--- | :----------
`enron-spam-hamham100` | A sample of 100 emails from the dataset that were classified as HAM.
`enron-spam-hamspam100` | A sample of 100 emails from the dataset that were classified as SPAM.

If you want load as raw text, one line per "record", use this Hive DDL, where we define two partitions, one for HAM and one for SPAM. We assume you have copied the `data/enron-spam-ham` directory to HDFS at `hdfs://server/data/enron-spam-ham`:

```
CREATE EXTERNAL TABLE IF NOT EXISTS mail (line STRING)
PARTITIONED BY (is_spam BOOLEAN);

ALTER TABLE mail ADD PARTITION(is_spam = true)
LOCATION 'hdfs://server/data/enron-spam-ham/spam100';

ALTER TABLE mail ADD PARTITION(is_spam = false)
LOCATION 'hdfs://server/data/enron-spam-ham/ham100';
```

Note that you could reformat the files into structured records to do more sophisticated processing of emails, such as separating out the headers, the "to:", "cc:", "bcc:", and the body. For example, the headers could stored in a Hive `MAP` and the recipients could be stored in `ARRAYs`.

## Shakespeare's Plays

The plain-text version of all of Shakespeare's plays, formatted exactly as you typically see them printed, i.e., using the conventional spacing and layout for plays.

Directory | Description
:--- | :----------
`shakespeare/all-shakespeare.txt` | The folio of Shakespeare's plays, as plain text.

To use from Hive as a source of unstructured text:

```
CREATE EXTERNAL TABLE IF NOT EXISTS shakespeare (line STRING)
LOCATION 'hdfs://server/data/shakespeare';
```

[Return](../tutorial/index.html) to the project [tutorial](../tutorial/index.html).


