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

Test program

```
import configparser
config = configparser.ConfigParser()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, unix_timestamp, window
spark = SparkSession.builder.config("spark.sql.streaming.schemaInference", True).getOrCreate()

stream = spark.readStream.format("ws").option("schema", "ticker").load()
# format = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"
# new_stream = stream.withColumn('timestamp', unix_timestamp().cast('timestamp'))

query = stream.select("side", "product_id", "last_size", "best_bid", "best_ask", "time").writeStream.format("console").outputMode("append").option("truncate", "false").start()

query.awaitTermination(5)

stream2 = spark.readStream.format("ws").option("schema", "heartbeat").load()
query2 = stream2.select("product_id", "sequence").writeStream.format("console").outputMode("append").option("truncate", "false").start()

query2.awaitTermination(5)
spark.stop()
```
