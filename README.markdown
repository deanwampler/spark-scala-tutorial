# Apache Spark Scala Tutorial - README

[![Join the chat at https://gitter.im/deanwampler/spark-scala-tutorial](https://badges.gitter.im/deanwampler/spark-scala-tutorial.svg)](https://gitter.im/deanwampler/spark-scala-tutorial?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

![](http://spark.apache.org/docs/latest/img/spark-logo-hd.png)

Dean Wampler, Ph.D.<br/>
[Lightbend](http://lightbend.com)<br/>
[dean.wampler@lightbend.com](mailto:dean.wampler@lightbend.com)<br/>
[@deanwampler](https://twitter.com/deanwampler)

This tutorial demonstrates how to write and run [Apache Spark](http://spark.apache.org) applications using Scala with some SQL. I also teach a little Scala as we go, but if you already know Spark and you are more interested in learning just enough Scala for Spark programming, see my other tutorial [Just Enough Scala for Spark](https://github.com/deanwampler/JustEnoughScalaForSpark).

This tutorial demonstrates how to write and run [Apache Spark](http://spark.apache.org) applications using Scala with some SQL. You can run the examples and exercises several ways:

1. [Jupyter notebooks](http://jupyter.org/) - The easiest way, especially for data scientists accustomed to _notebooks_
2. In an IDE, like IntelliJ - Familiar for developers
3. At the terminal prompt using the build tool [SBT](https://www.scala-sbt.org/)

This tutorial is mostly about learning Spark, but I teach you a little Scala as we go. If you are more interested in learning just enough Scala for Spark programming, see my new tutorial [Just Enough Scala for Spark](https://github.com/deanwampler/spark-scala-tutorial).

For more advanced Spark training and for information about Lightbend's _Fast Data Platform_, please visit [lightbend.com/fast-data-platform](http://www.lightbend.com/platform/fast-data-platform).

## Acknowledgments

I'm grateful that several people have provided feedback, issue reports, and pull requests. In particular:

* [Ivan Mushketyk](https://github.com/mushketyk)
* [Andrey Batyuk](https://github.com/abatyuk)
* [Colin Jones](https://github.com/trptcolin)
* [Lutz Hühnken](https://github.com/lutzh)

## Getting Help

Before describing the different ways to work with the tutorial, if you're having problems, use the [Gitter chat room](https://gitter.im/deanwampler/spark-scala-tutorial) to ask for help. If you're reasonably certain you've found a bug, post an issue to the [GitHub repo](https://github.com/deanwampler/spark-scala-tutorial/issues). Pull requests are welcome!!

## Setup Instructions

Let's get started...

### Download the Tutorial

Begin by cloning or downloading the tutorial GitHub project [github.com/deanwampler/spark-scala-tutorial](https://github.com/deanwampler/spark-scala-tutorial).

Now Pick the way you want to work through the tutorial:

1. Jupyter notebooks - Go [here](#use-jupyter-notebooks)
2. In an IDE, like IntelliJ - Go [here](#use-ide)
3. At the terminal prompt using SBT - Go [here](#use-sbt)

<a name="use-jupyter-notebooks"></a>
## Using Jupyter Notebooks

The easiest way to work with this tutorial is to use a [Docker](https://docker.com) image that combines the popular [Jupyter](http://jupyter.org/) notebook environment with all the tools you need to run Spark, including the Scala language. It's called the [All Spark Notebook](https://hub.docker.com/r/jupyter/all-spark-notebook/).  It bundles [Apache Toree](https://toree.apache.org/) to provide Spark and Scala access.

There are other notebook tools you might investigate for your team's needs:

* [Jupyter](https://ipython.org/) + [BeakerX](http://beakerx.com/) - a powerful set of extensions for Jupyter
* [Zeppelin](http://zeppelin-project.org/) - a popular environment in big data environments
* [Spark Notebook](http://spark-notebook.io) - a powerful, but not as polished
* [IBM Data Science Experience](http://datascience.ibm.com/) - IBM's full-featured environment for data science
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
  -v "$PWD/data":/home/jovyan/data \
  -v "$PWD/notebooks":/home/jovyan/notebooks \
  jupyter/all-spark-notebook
```

The Windows `run.bat` command executes this command (wrapped for readability):

```bash
docker run -it --rm
  -p 8888:8888 -p 4040:4040
  -v "%CD%\data":/home/jovyan/data
  -v "%CD%\notebooks":/home/jovyan/notebooks
  jupyter/all-spark-notebook
```

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

Now copy and paste the URL shown in a browser window.

> **Warning:** When you quit the Docker container at the end of the tutorial, all your changes will be lost, unless they are in the `data` and `notebooks` directories shown in the Jupyter UI! To save notebooks elsewhere, export the notebook using the _File > Download as > Notebook_ menu item in toolbar.

## Running the Tutorial

Now we need to load the tutorial into Jupyter.

* Click the _Upload_ button on the upper right-hand side of the UI.
* Browse to where you downloaded and expanded the tutorial, then to the `notebooks` directory. \
* Open `spark-scala-tutorial`.
* Click the blue _Upload_ button.
* Click the link for the tutorial that is now shown in the list of notebooks.

It opens in a new browser tab. It will take several seconds to load. (It's big!)

>  **Tip:** If the new tab fails to open or the notebook fails to load as shown, check the terminal window where you started Jupyter. Are there any error messages?

Finally, you'll notice there is a box around the first "cell". This cell has one line of source code `println("Hello World!")`. Above this cell is a toolbar with a button that has a right-pointing arrow and the word _run_. Click that button to run this code cell. Or, use the menu item _Cell > Run Cells_.

After many seconds, once initialization has completed, it will print the output, `Hello World!` just below the input text field.

Do the same thing for the next box. It should print `Array(shakespeare)`.

> **Warning:** If instead you see `Array()` or `null` is printed, the mounting of the `data` directory did not work correctly. In the terminal window, use `control-c` to exit from the Docker container, make sure you are in the root directory of the project (`data` should be a subdirectory), restart the docker image, and make sure you enter the command exactly as shown.

You are now ready to go through the [tutorial](Tutorial.markdown).

<a name="use-ide"></a>
### Use an IDE

TODO

You are now ready to go through the [tutorial](Tutorial.markdown).

<a name="use-sbt"></a>
### Use SBT in a Terminal

TODO

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
