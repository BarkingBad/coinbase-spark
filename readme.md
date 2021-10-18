# OpenAQ streamer

## Build

- Build docker image
```bash
make image
```

- Run docker
```bash
make run
```

## Run spark structured streaming query
After running the docker container a link to jupiter notebook will be displayed.
Without any extra configuration one starting with `http://127.0.0.1:8888...` should work.

After opening jupiter notebook in your browser go to work folder and open `Notebook.ipynb`.
It contains a fairly simple example on how to run a Spark Structured Streaming app from python.
It is possible also to do the same thing with Scala or R, but you need to adjust the code in the notebook.

Please keep in mind that you need to manually stop the session using `spark.stop()`.
Even though some exceptions may occur this closes the websocket session correctly.
