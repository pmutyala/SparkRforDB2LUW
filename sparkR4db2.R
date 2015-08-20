# Written by pmutyala@ca.ibm.com
if(exists("sc")) sparkR.stop() # just in case if there is earlier sc exists
Sys.setenv(SPARK_HOME="/usr/local/bin/spark-1.4.0-bin-hadoop2.6")
.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib"), .libPaths()))
library(SparkR)
sc <- sparkR.init(appName = "db2SparkRSupportExample",master="local")
sqlContext <- sparkRSQL.init(sc)
# Install RJDBC packages if not available
if(!require(RJDBC)) install.packages("RJDBC",dependencies = TRUE)
library(RJDBC)
drvpath <- file.path(Sys.getenv("SPARK_HOME"),"db2_driver/java/db2jcc4.jar")
drv <- JDBC("com.ibm.db2.jcc.DB2Driver",drvpath)
cxn <- dbConnect(drv,"jdbc:db2://<hostname>:<listenerport>/<dbname>","<username>","<password>")
db2df <- dbGetQuery(cxn,'select * from "PMUTYALA"."HEART"')
class(db2df)
#create sparkR dataframe from the db2 one
sparkdf <- createDataFrame(sqlContext, db2df)
class(sparkdf)
head(sparkdf)



