# Apache Spark Scala Tutorial - README

[![Join the chat at https://gitter.im/deanwampler/spark-scala-tutorial](https://badges.gitter.im/deanwampler/spark-scala-tutorial.svg)](https://gitter.im/deanwampler/spark-scala-tutorial?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

![](http://spark.apache.org/docs/latest/img/spark-logo-hd.png)

Dean Wampler<br/>
[deanwampler@gmail.com](mailto:deanwampler@gmail.com)<br/>
[@deanwampler](https://twitter.com/deanwampler)

This tutorial demonstrates how to write and run [Apache Spark](http://spark.apache.org) applications using Scala with some SQL. I also teach a little Scala as we go, but if you already know Spark and you are more interested in learning just enough Scala for Spark programming, see my other tutorial [Just Enough Scala for Spark](https://github.com/deanwampler/JustEnoughScalaForSpark).

You can run the examples and exercises several ways:

1. Notebooks, like [Jupyter](http://jupyter.org/) - The easiest way, especially for data scientists accustomed to _notebooks_.
2. In an IDE, like [IntelliJ](https://www.jetbrains.com/idea/) - Familiar for developers.
3. At the terminal prompt using the build tool [SBT](https://www.scala-sbt.org/).

This tutorial is mostly about learning Spark, but I teach you a little Scala as we go. If you are more interested in learning just enough Scala for Spark programming, see my new tutorial [Just Enough Scala for Spark](https://github.com/deanwampler/spark-scala-tutorial).

> **Notes:** 
>
> 1. The current version of Spark used is 2.3.X, which is a bit old. (TODO!)
> 2. While the notebook approach is the easiest way to use this tutorial to learn Spark, the IDE and SBT options show details for creating Spark _applications_, i.e., writing executable programs you build and run, as well as examples that use the interactive Spark Shell.

## Acknowledgments

I'm grateful that several people have provided feedback, issue reports, and pull requests. In particular:

* [Ivan Mushketyk](https://github.com/mushketyk)
* [Andrey Batyuk](https://github.com/abatyuk)
* [Colin Jones](https://github.com/trptcolin)
* [Lutz Hühnken](https://github.com/lutzh)

## Getting Help

Before describing the different ways to work with the tutorial, if you're having problems, use the [Gitter chat room](https://gitter.im/deanwampler/spark-scala-tutorial) to ask for help. You can also use the new [Discussions feature](https://github.com/deanwampler/spark-scala-tutorial/discussions) for the GitHub repo. If you're reasonably certain you've found a bug, post an issue to the [GitHub repo](https://github.com/deanwampler/spark-scala-tutorial/issues). Pull requests are welcome, too!!

## Setup Instructions

Let's get started...

### Download the Tutorial

Begin by cloning or downloading the tutorial GitHub project [github.com/deanwampler/spark-scala-tutorial](https://github.com/deanwampler/spark-scala-tutorial).

Now Pick the way you want to work through the tutorial:

1. Notebooks - Go [here](#use-notebooks)
2. In an IDE, like IntelliJ - Go [here](#use-ide)
3. At the terminal prompt using SBT - Go [here](#use-sbt)

<a name="use-notebooks"></a>
## Using Notebooks

The easiest way to work with this tutorial is to use a [Docker](https://docker.com) image that combines the popular [Jupyter](http://jupyter.org/) notebook environment with all the tools you need to run Spark, including the Scala language. It's called the [all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook/).  It bundles [Apache Toree](https://toree.apache.org/) to provide Spark and Scala access.
The [webpage](https://hub.docker.com/r/jupyter/all-spark-notebook/) for this Docker image discusses useful information like using Python as well as Scala, user authentication topics, running your Spark jobs on clusters, rather than local mode, etc.

There are other notebook options you might investigate for your needs:

**Open source:**

* [Polynote](https://polynote.org/) - A cross-language notebook environment with built-in Scala support. Developed by Netflix.
* [Jupyter](https://ipython.org/) + [BeakerX](http://beakerx.com/) - a powerful set of extensions for Jupyter.
* [Zeppelin](http://zeppelin-project.org/) - a popular tool in big data environments

**Commercial:**

* [Databricks](https://databricks.com/) - a feature-rich, commercial, cloud-based service

## Installing Docker and the Jupyter Image

If you need to install Docker, follow the installation instructions at [docker.com](https://www.docker.com/products/overview) (the _community edition_ is sufficient).

Now we'll run the docker image. It's important to follow the next steps carefully. We're going to mount two local directories inside the running container, one for the data we want to use so and one for the notebooks.

* Open a terminal or command window
* Change to the directory where you expanded the tutorial project or cloned the repo
* To download and run the Docker image, run the following command: `run.sh` (MacOS and Linux) or `run.bat` (Windows)

The MacOS and Linux `run.sh` command executes this command:

```bash
docker run -it --rm \
  -p 8888:8888 -p 4040:4040 \
  --cpus=2.0 --memory=2000M \
  -v "$PWD/data":/home/jovyan/data \
  -v "$PWD/notebooks":/home/jovyan/notebooks \
  "$@" \
  jupyter/all-spark-notebook
```

The Windows `run.bat` command is similar, but uses Windows conventions.

The `--cpus=... --memory=...` arguments were added because the notebook "kernel" is prone to crashing with the default values. Edit to taste. Also, it will help to keep only one notebook (other than the Introduction) open at a time.

The `-v PATH:/home/jovyan/dir` tells Docker to mount the `dir` directory under your current working directory, so it's available as `/home/jovyan/dir` inside the container. _This is essential to provide access to the tutorial data and notebooks_. When you open the notebook UI (discussed shortly), you'll see these folders listed.

> **Note:** On Windows, you may get the following error: _C:\Program Files\Docker\Docker\Resources\bin\docker.exe: Error response from daemon: D: drive is not shared. Please share it in Docker for Windows Settings."_ If so, do the following. On your tray, next to your clock, right-click on Docker, then click on Settings. You'll see the _Shared Drives_. Mark your drive and hit apply. See [this Docker forum thread](https://forums.docker.com/t/cannot-share-drive-in-windows-10/28798/5) for more tips.

The `-p 8888:8888 -p 4040:4040` arguments tells Docker to "tunnel" ports 8888 and 4040 out of the container to your local environment, so you can get to the Jupyter UI at port 8888 and the Spark driver UI at 4040.

You should see output similar to the following:

```bash
Unable to find image 'jupyter/all-spark-notebook:latest' locally
latest: Pulling from jupyter/all-spark-notebook
e0a742c2abfd: Pull complete
...
ed25ef62a9dd: Pull complete
Digest: sha256:...
Status: Downloaded newer image for jupyter/all-spark-notebook:latest
Execute the command: jupyter notebook
...
[I 19:08:15.017 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 19:08:15.019 NotebookApp]

    Copy/paste this URL into your browser when you connect for the first time,
    to login with a token:
        http://localhost:8888/?token=...
```

Now copy and paste the URL shown in a browser window. (Use command+click in your terminal window on MacOS.)

> **Warning:** When you quit the Docker container at the end of the tutorial, all your changes will be lost, unless they are in the `data` and `notebooks` directories that we mounted! To save notebooks you defined in other locations, export them using the _File > Download as > Notebook_ menu item in toolbar.

## Running the Tutorial

In the Jupyter UI, you should see three folders, `data`, `notebooks`, and `work`. The first two are the folders we mounted. The data we'll use is in the `data` folder. The notebooks we'll use are... you get the idea.

Open the `notebooks` folder and click the link for `00_Intro.ipynb`.

It opens in a new browser tab. It may take several seconds to load.

>  **Tip:** If the new tab fails to open or the notebook fails to load as shown, check the terminal window where you started Jupyter. Are there any error messages?

If you're new to Jupyter, try _Help > User Interface Tour_ to learn how to use Jupyter. At a minimum, you need to new that the content is organized into _cells_. You can navigate with the up and down arrows or clicks. When you come to a cell with code, either click the _run_ button in the toolbar or use shift+return to execute the code.

Read through the Introduction notebook, then navigate to the examples using the table near the bottom. I've set up the table so that clicking each link opens a new browser tab.

<a name="use-ide"></a>
### Use an IDE

The tutorial is also set up as a using the build tool [SBT](https://www.scala-sbt.org/). The popular IDEs, like [IntelliJ](https://www.jetbrains.com/idea/) with the Scala plugin (required) and [Eclipse with Scala](http://scala-ide.org/), can import an SBT project and automatically create an IDE project from it.

Once imported, you can run the Spark job examples as regular applications. There are some examples implemented as scripts that need to be run using the Spark Shell or the SBT console. The tutorial goes into the details.

You are now ready to go through the [tutorial](Tutorial.markdown).

<a name="use-sbt"></a>
### Use SBT in a Terminal

Using [SBT](https://www.scala-sbt.org/) in a terminal is a good approach if you prefer to use a code editor like Emacs, Vim, or SublimeText. You'll need to [install SBT](https://www.scala-sbt.org/download.html), but not Scala or Spark. Those dependencies will be resolved when you build the software.

Start the `sbt` console, then build the code, where the `sbt:spark-scala-tutorial>` is the prompt I've configured for the project. Running `test` compiles the code and runs the tests, while `package` creates a jar file of the compiled code and configuration files:

```shell
$ sbt
...
sbt:spark-scala-tutorial> test
...
sbt:spark-scala-tutorial> package
...
sbt:spark-scala-tutorial>
```

You are now ready to go through the [tutorial](Tutorial.markdown).

## Going Forward from Here

To learn more, see the following resources:

* [Lightbend's Fast Data Platform](http://lightbend.com/fast-data-platform) - a curated, fully-supported distribution of open-source streaming and microservice tools, like Spark, Kafka, HDFS, Akka Streams, etc.
* The Apache Spark [website](http://spark.apache.org/).
* [Talks from the Spark Summit conferences](http://spark-summit.org).
* [Learning Spark](http://shop.oreilly.com/product/0636920028512.do), an excellent introduction from O'Reilly, if now a bit dated.

## Final Thoughts

Thank you for working through this tutorial. Feedback and pull requests are welcome.

[Dean Wampler](mailto:dean.wampler@lightbend.com)
