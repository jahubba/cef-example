import org.apache.spark.sql.functions._
  
val cefDataFrame = spark.read.
  format("nl.anchormen.spark.cef.CefSource").
  option("ignore.exception", "true").
  load("/tmp/cef")
  
cefDataFrame.cache
cefDataFrame.count
cefDataFrame.show
cefDataFrame.filter("rt is not null").count
val espn = cefDataFrame.filter("request like '%espn%'")
espn.count
espn.show(false)