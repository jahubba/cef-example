val cefDataFrame = spark.read.
  format("nl.anchormen.spark.cef.CefSource").
  option("ignore.exception", "true").
  load("/tmp/cef")

val cefWithDate = cefDataFrame.
  withColumn("artDate", to_date(cefDataFrame("art")))
  
cefWithDate.write.
  mode("overwrite").
  partitionBy("artDate").
  parquet("/tmp/cef-parquet")