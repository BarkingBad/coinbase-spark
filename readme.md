# CoinBase Spark

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

```
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, unix_timestamp, window

spark = SparkSession.builder.config("spark.sql.streaming.schemaInference", True).getOrCreate()

stream = spark.readStream.format("ws").option("schema", "ticker").load()
query = stream.select("side", "product_id", "last_size", "best_bid", "best_ask", "time").writeStream.format("console").outputMode("append").option("truncate", "false").start()

query.awaitTermination(10)
query.stop()
spark.stop()
```

If one start new query and accidentally lose the reference for it before stopping (e.g. python interpreter will fail on some instruction before stopping), he/she should kill spark session by explicitly calling `spark.stop()` in a new cell. Otherwise, the websockets will still be alive and will stream the data.

## Usage

The libary exposes few channels from the [CoinBase WS API](https://docs.cloud.coinbase.com/exchange/docs/channels)
Currently implemented channels are:
- heartbeat
- ticker
- status
- level2
- auction

To subscribe to specific channel one has to pass `schema` option value:
`spark.readStream.format("ws").option("schema", "ticker").load()`

Some channels can be furtherly configured:


