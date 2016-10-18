from __future__ import print_function
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.ml.feature import MinMaxScaler,VectorAssembler
from pyspark.ml.linalg import Vectors,VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql import SQLContext
if __name__=="__main__":
    spark=SparkSession\
        .builder\
        .appName("LR_FOR_TIANCHI")\
        .enableHiveSupport()\
        .master("spark://master:7077")\
        .getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://master:9000/checkpoint/")
    DiskLevel=StorageLevel.DISK_ONLY
    udfunction=udf(lambda column: Vectors.dense(column),VectorUDT())
    spark.sql("use itemRecommend")
    OriginalFeatures=spark.sql("select * from feature_table")
    columns=OriginalFeatures.columns
    VectorFeatures=OriginalFeatures
    #VectorFeatures=VectorFeatures.withColumn("i1",udfunction(VectorFeatures["i1"]))
    #VectorFeatures.show()
    i=0
    for column in columns: 
        if column != "tag":
            i=i+1
            print(column)
            VectorFeatures=VectorFeatures.withColumn(column,udfunction(VectorFeatures[column]))
            #VectorFeatures.persist(storageLevel=DiskLevel)
            #VectorFeatures.show(
            if i==30:
                i=0
                #VectorFeatures.persist(storageLevel=DiskLevel)
                rddFeature=VectorFeatures.rdd
                rddFeature.checkpoint()
                rddFeature.count()
                VectorFeatures=rddFeature.toDF()
    VectorFeatures.write.saveAsTable("FeaturesLR","parquet","overwrite")
    '''
    print(VectorFeatures.first())
    #i=0
    for column in columns:
        if column != "tag":
            i=i+1
            print(column)
            mmScaler=MinMaxScaler(inputCol=column,outputCol=column+"Scalerd")
            model=mmScaler.fit(VectorFeatures)
            VectorFeatures=model.transform(VectorFeatures)
            #VectorFeatures.persist(storageLevel=DiskLevel)
            #if i==50:
             #   i=0
              #  print(VectorFeatures.show)
    columns=VectorFeatures.columns
    columns.remove("tag")
    vec = VectorAssembler(inputCols=columns, outputCol="features")
    VectorsFeature = vec.transform(VectorFeatures)
    LastFeatures=VectorFeatures.select("tag","features")
    LastFeatures.write.savaAsTable("FeaturesLR","parquet","overwrite")
    '''
