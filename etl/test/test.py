from etl.util.Main import *
import unittest

class TestMyFunc(unittest.TestCase):

    def testDF1(self):

        def chackDF1(spark):
            data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1), \
                    ("Refrigerator", "1648770999000", 35000, " LG ", "null", 2), \
                    ("Air Cooler", "1648770948000", 45000, " Voltas ", "null", 3)]
            schema = StructType([StructField("Product Name", StringType(), True), \
                                 StructField("Issue Date", StringType(), True), \
                                 StructField("Price", LongType(), True), \
                                 StructField("Brand", StringType(), True), \
                                 StructField("Country", StringType(), True), \
                                 StructField("ProductNumber", StringType(), False)
                                 ])
            df = spark.createDataFrame(data, schema)
            return df

        self.assertEqual(CreateDataframe().collect(), chackDF1(spark).collect())

    def testDF2(self):

        def chackDF2(spark):
            data2 = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1),
                     (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 2),
                     (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 3)]
            schema2 = StructType([StructField("SourceID", LongType(), True),
                                  StructField("TransactionNumber", LongType(), True),
                                  StructField("Language", StringType(), True),
                                  StructField("ModelNumber", LongType(), True),
                                  StructField("StartTime", StringType(), True),
                                  StructField("ProductNumber", IntegerType(), True)])
            df = spark.createDataFrame(data2, schema2)
            return df

        self.assertEqual(CreateDataframe2().collect(), chackDF2(spark).collect())


    def testTimestampFormat(self):

        def timestamp(spark):
            data3 = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1, "2022-04-01 05:25:33"), \
                     ("Refrigerator", "1648770999000", 35000, " LG ", "null", 2, "2022-04-01 05:26:39"), \
                     ("Air Cooler", "1648770948000", 45000, " Voltas ", "null", 3, "2022-04-01 05:25:48")]
            schema3 = StructType([StructField("Product Name", StringType(), True), \
                                  StructField("Issue Date", StringType(), True), \
                                  StructField("Price", LongType(), True), \
                                  StructField("Brand", StringType(), True), \
                                  StructField("Country", StringType(), True), \
                                  StructField("ProductNumber", StringType(), False), \
                                  StructField("timestamp", StringType(), True)
                                  ])
            df = spark.createDataFrame(schema=schema3, data=data3)
            return df

        self.assertEqual(Timestamp().collect(), timestamp(spark).collect())


    def testDate(self):

        def chackdate(spark):
            data4 = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1, "2022-04-01 05:25:33", "2022-04-01"), \
                    ("Refrigerator", "1648770999000", 35000, " LG ", "null", 2, "2022-04-01 05:26:39", "2022-04-01"), \
                    ("Air Cooler", "1648770948000", 45000, " Voltas ", "null", 3, "2022-04-01 05:25:48", "2022-04-01")]
            schema4 = StructType([StructField("Product Name", StringType(), True), \
                                 StructField("Issue Date", StringType(), True), \
                                 StructField("Price", LongType(), True), \
                                 StructField("Brand", StringType(), True), \
                                 StructField("Country", StringType(), True), \
                                 StructField("ProductNumber", StringType(), False), \
                                 StructField("timestamp", StringType(), True),
                                 StructField("date", StringType(), True)
                                 ])
            df = spark.createDataFrame(data4, schema4)
            return df

        self.assertEqual(DateTime().collect(), chackdate(spark).collect())

    def testEmptySpaceColumn(self):

        def chackEmptySpace(spark):
            data5 = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1), \
                    ("Refrigerator", "1648770999000", 35000, "LG", "null", 2), \
                    ("Air Cooler", "1648770948000", 45000, "Voltas", "null", 3)]
            schema5 = StructType([StructField("Product Name", StringType(), True), \
                                 StructField("Issue Date", StringType(), True), \
                                 StructField("Price", LongType(), True), \
                                 StructField("Brand", StringType(), True), \
                                 StructField("Country", StringType(), True), \
                                 StructField("ProductNumber", StringType(), False)
                                 ])
            df = spark.createDataFrame(data5, schema5)

            return df

        self.assertEqual(EmptySpaces().collect(), chackEmptySpace(spark).collect())

    def testReplaceEmpty(self):

        def chackReplaceValue(spark):
            data6 = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1), \
                     ("Refrigerator", "1648770999000", 35000, " LG ", "", 2), \
                     ("Air Cooler", "1648770948000", 45000, " Voltas ", "", 3)]
            schema6 = StructType([StructField("Product Name", StringType(), True), \
                                  StructField("Issue Date", StringType(), True), \
                                  StructField("Price", LongType(), True), \
                                  StructField("Brand", StringType(), True), \
                                  StructField("Country", StringType(), True), \
                                  StructField("ProductNumber", StringType(), False)
                                  ])

            df = spark.createDataFrame(data6, schema6)
            return df

        self.assertEqual(ReplacingEmpty().collect(), chackReplaceValue(spark).collect())


    def testCamelCase(self):

        def chackSnakeCase(spark):
            data7 = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1),
                     (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 2),
                     (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 3)]
            schema7 = StructType([StructField("source_i_d", LongType(), True),
                                  StructField("transaction_number", LongType(), True),
                                  StructField("language", StringType(), True),
                                  StructField("model_number", LongType(), True),
                                  StructField("start_time", StringType(), True),
                                  StructField("product_number", IntegerType(), True)])

            df = spark.createDataFrame(data7, schema7)

            return df
        self.assertEqual(ChangeCamelCase().collect(), chackSnakeCase(spark).collect())

    def testStatTime(self):

        def chackStartTime(spark):
            data8 = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1, 1640573429000),
                     (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 2, 1640573474000),
                     (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 3, 1640573562000)]
            schema8 = StructType([StructField("source_i_d", LongType(), True),
                                  StructField("transaction_number", LongType(), True),
                                  StructField("language", StringType(), True),
                                  StructField("model_number", LongType(), True),
                                  StructField("start_time", StringType(), True),
                                  StructField("product_number", IntegerType(), True),
                                  StructField("start_time_ms", LongType(), True)])

            df = spark.createDataFrame(data8, schema8)
            return df

        self.assertEqual(StartTime().collect(), chackStartTime(spark).collect())

    def testJoinTables(self):

        def chackJoinTables(spark):
            data = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", 1), \
                    ("Refrigerator", "1648770999000", 35000, " LG ", "null", 2), \
                    ("Air Cooler", "1648770948000", 45000, " Voltas ", "null", 3)]
            schema = StructType([StructField("Product Name", StringType(), True), \
                                 StructField("Issue Date", StringType(), True), \
                                 StructField("Price", LongType(), True), \
                                 StructField("Brand", StringType(), True), \
                                 StructField("Country", StringType(), True), \
                                 StructField("ProductNumber", StringType(), False)
                                 ])
            df = spark.createDataFrame(data, schema)
            data2 = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", 1),
                     (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", 2),
                     (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", 3)]
            schema2 = StructType([StructField("SourceID", LongType(), True),
                                  StructField("TransactionNumber", LongType(), True),
                                  StructField("Language", StringType(), True),
                                  StructField("ModelNumber", LongType(), True),
                                  StructField("StartTime", StringType(), True),
                                  StructField("ProductNumber", IntegerType(), True)])
            df2 = spark.createDataFrame(data2, schema2)
            df3 = df.join(df2, df.ProductNumber == df2.ProductNumber, "full")
            return df3

        self.assertTrue(Join(), chackJoinTables(spark))




