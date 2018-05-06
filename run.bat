docker run -it --rm -p 8888:8888 -p 4040:4040 --cpus=2.0 --memory=2000M -v "%CD%\data":/home/jovyan/data -v "%CD%\notebooks":/home/jovyan/notebooks jupyter/all-spark-notebook
