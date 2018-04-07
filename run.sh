docker run -it --rm -p 8888:8888 \
  -v "$PWD/data":/home/jovyan/data \
  -v "$PWD/notebooks":/home/jovyan/notebooks \
  jupyter/all-spark-notebook
