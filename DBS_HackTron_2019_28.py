import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import unix_timestamp
from pyspark.sql import SQLContext



CUSTOMER = spark.read.option('delimiter',"|").csv(r"C:\Users\pooja\Desktop\DBS\Customer.txt")

CUSTOMER_EXTN = spark.read.option('delimiter',"|").csv(r"C:\Users\pooja\Desktop\DBS\Customer_Extended.txt")
SALES = spark.read.option('delimiter',"|").csv(r"C:\Users\pooja\Desktop\DBS\Sales.txt")
REFUND = spark.read.option('delimiter',"|").csv(r"C:\Users\pooja\Desktop\DBS\Refund.txt")
PRODUCT = spark.read.option('delimiter',"|").csv(r"C:\Users\pooja\Desktop\DBS\Product.txt")

CUSTOMER_EXTN=CUSTOMER_EXTN.dropDuplicates()
SALES=SALES.dropDuplicates()
REFUND=REFUND.dropDuplicates()
PRODUCT=PRODUCT.dropDuplicates()

PRODUCT_FINAL=PRODUCT.select((col('_c0')).alias('product_id'),
                             col('_c1').alias('product_name'),
                             col('_c2').alias('product_type'),
                             col('_c3').alias('product_price'))
							 
CUSTOMER_FINAL = CUSTOMER.select((col('_c0')).alias('customer_id'),
                             col('_c1').alias('customer_first_name'),
                             col('_c2').alias('customer_last_name'),
                             col('_c3').alias('phone_number'))

SALES_FINAL = SALES.select((col('_c0')).alias('transaction_id'),
                             col('_c1').alias('customer_id'),
                             col('_c2').alias('product_id'),
                             col('_c3').alias('timestamp'),
                            col('_c4').alias('total_amount'),
                            col('_c5').alias('total_quantity'))
							
REFUND_FINAL = REFUND.select((col('_c0')).alias('refund_id'),
                             col('_c1').alias('original_transaction_id'),
                             col('_c2').alias('customer_id'),
                             col('_c3').alias('product_id'),
                            col('_c4').alias('timestamp'),
                             col('_c5').alias('refund_amount'),
                            col('_c6').alias('refund_quantity'))

PRODUCT_FINAL.createOrReplaceTempView("product")
REFUND_FINAL.createOrReplaceTempView("refund")
SALES_FINAL.createOrReplaceTempView("sales")
CUSTOMER_FINAL.createOrReplaceTempView("customer")
							
                             							 

#solution for last query
USER_COUNT=spark.sql("select count(*) from (select count(*),customer_id,product_id,TO_DATE(CAST(UNIX_TIMESTAMP(timestamp, 'MM/dd/yyyy') AS TIMESTAMP)) from sales group by customer_id,product_id,TO_DATE(CAST(UNIX_TIMESTAMP(timestamp, 'MM/dd/yyyy') AS TIMESTAMP)) having count(*)>=2)a")
USER_COUNT.show()

#solution for  5th one
PRODUCT_NO_SALE=spark.sql("select product.product_id, product.product_name from product where product.product_id not in (select distinct product_id from sales)")

#solution for 2nd one
SALES_DISTRIBUTION=spark.sql("select p.product_id, p.product_name, p.product_type, s.total_quantity from product p left join  sales s on (p.product_id = s.product_id)")

#solution for 3rd one
TOTAL_AMOUNT=spark.sql("select sum (allAmnt) from (select transaction_id,cast(substring(total_amount, 2, length(total_amount)) as float) allAmnt  from sales where year(TO_DATE(CAST(UNIX_TIMESTAMP(timestamp, 'MM/dd/yyyy') AS TIMESTAMP)))=2013 and transaction_id not in (select original_transaction_id from refund))a ")

#we have used pandas for getting visualization(did it for one dataframe)