import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class ecomdata:

    def __init__(self, storage_account_name="", storage_account_key="", container="",
                 spark=SparkSession.builder.appName("myspark").getOrCreate()):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.container = container
        self.spark = spark
        logging.info("We have initiated storage account name")
        logging.info("We have initiated storage account key")
        logging.info("We have initiated container")

    def top_selling_products(self):
        try:
            query_top_selling = "select count(o.order_id) as total_orders,p.product_id from products p" \
                                " inner join items i on i.product_id=p.product_id inner join orders o " \
                                "on o.order_id=i.order_id group by p.product_id order by count(o.order_id) desc"

            logging.info("Display top selling products")
            return self.spark.sql(query_top_selling)
        except:

            print("Correct the problem in Query")
            logging.info("Query has error")

    def total_revenue_years(self, start_date='2017-01-01', end_date='2018-01-01'):
        try:
            query_rev_years = "select sum(p.payment_value) as total_revenue_generated from orders o" \
                              " inner join payments p on p.order_id=o.order_id " \
                              "where o.order_delivered_customer_date between '{0}' and '{1}'".format(start_date,
                                                                                                     end_date)

            return self.spark.sql(query_rev_years)

        except:
            print("Correct the problem in query")
            logging.info("Query has an error")

    def orders_product_category(self):
        try:
            query_ord_prod_cat = "select p.product_category_name,count(i.order_id) as total_orders  from items i" \
                                 " inner join products p on p.product_id=i.product_id group by p.product_category_name"
            return self.spark.sql(query_ord_prod_cat)

        except:
            print("Correct the problem in query")
            logging.info("Query has an error")

    def No_customers_by_region(self):
        try:
            query_cust_region = "select g.geolocation_state as Region_by_state,count(c.customer_id) as Customer " \
                                "from geolocation g inner join customers c group by g.geolocation_state "
            return self.spark.sql(query_cust_region)
        except:
            print("Correct the problem in query")
            logging.info("Query has an error")

    def rev_anually(self):
        try:
            query_rev_annual = "select sum(p.payment_value) as revenue,year(o.order_delivered_customer_date) " \
                               "as yearly from orders o inner join payments p on o.order_id=p.order_id " \
                               "where o.order_status='delivered'  group by year(o.order_delivered_customer_date)"
            return self.spark.sql(query_rev_annual)
        except:
            print("Correct the problem in query")
            logging.info("Query has an error")

    def m_valued_salesman(self):
        try:
            query_most_val_sal = "select s.seller_id,avg(r.review_score) as high_review_score" \
                                 ",count(r.review_score) as all_reviews from sellers s inner join items i" \
                                 " on i.seller_id=s.seller_id inner join orders o on o.order_id=i.order_id " \
                                 "inner join reviews r on r.order_id=o.order_id  group by s.seller_id" \
                                 " order by count(r.review_score) desc "
            return self.spark.sql(query_most_val_sal)

        except:
            print("Correct the problem in query")
            logging.info("Query has an error")

    def m_valued_customer(self):
        try:
            query_most_val_cust = " select count(o.order_id) as total_orders_placed,c.customer_id,s.seller_id " \
                                  " from customers c inner join orders o on o.customer_id=c.customer_id inner join" \
                                  " items i on i.order_id=o.order_id inner join sellers s on s.seller_id=i.seller_id" \
                                  " group by c.customer_id,s.seller_id order by count(o.order_id) desc"

            return self.spark.sql(query_most_val_cust)

        except:
            print("Correct the problem in query")
            logging.info("Query has an error")

    def prod_scop(self):
        try:
            query_scop = "select sum(p.payment_value) as product_sales,pr.product_id,day(o.order_purchase_timestamp)" \
                         " as day,month(o.order_purchase_timestamp) as month,year(o.order_purchase_timestamp) as year" \
                         " from payments p inner join orders o on o.order_id=p.order_id inner join items i" \
                         " on i.order_id=o.order_id inner join products pr on pr.product_id=i.product_id" \
                         " group by day(o.order_purchase_timestamp),pr.product_id,month(o.order_purchase_timestamp)" \
                         ",year(o.order_purchase_timestamp) order by year(o.order_purchase_timestamp)," \
                         "month(o.order_purchase_timestamp),day(o.order_purchase_timestamp)"
            return self.spark.sql(query_scop)
        except:
            print("Correct the problem in query")

    def min_max_priced_products(self):
        try:
            query_max = " select p.product_id ,max(i.price) OVER () as max_price " \
                        " from items i inner join products p on i.product_id=p.product_id"
            query_min = " select p.product_id ,min(i.price) OVER () as min_price " \
                        " from items i inner join products p on i.product_id=p.product_id"
            return self.spark.sql(query_max), self.spark.sql(query_min)
        except:
            print("Correct the problem in query")

    def orders_by_region_city(self):
        try:
            query_reg_city = "select count(o.order_id) as total_orders,g.geolocation_city,g.geolocation_state from orders" \
                             " o inner join items i on i.order_id=o.order_id inner join geolocation g" \
                             " group by g.geolocation_city,g.geolocation_state "
            return self.spark.sql(query_reg_city)
        except:
            print("Correct the problem in query")

    def reviewed_product(self):
        try:
            query_rev_prod = "select p.product_id,avg(r.review_score) as avg_rev_score from products p inner join " \
                             "items i on p.product_id=i.product_id inner join orders o on o.order_id=i.order_id  " \
                             "inner join reviews r on o.order_id=r.order_id group by p.product_id order by " \
                             "avg_rev_score desc "
            return self.spark.sql(query_rev_prod)
        except:
            print("Correct the problem in query")

    def total_orders_customers(self):
        try:
            query_tot_ord_cust = "select count(o.order_id) as total_orders,c.customer_id from orders o" \
                                 " inner join customers c on c.customer_id=o.customer_id inner join items i" \
                                 " on i.order_id=o.order_id group by c.customer_id order by count(o.order_id) desc"

            logging.info("performed operation of spark sql of total_orders_by_customers")
            return self.spark.sql(query_tot_ord_cust)

        except:
            print("error in query")
            logging.error("error occured")

    def shipment_age(self):
        try:
            query_ship_age = "select datediff(i.shipping_limit_date,o.order_purchase_timestamp) as date_diff" \
                             ",o.order_purchase_timestamp,i.shipping_limit_date from items i" \
                             " inner join orders o on o.order_id=i.order_id"
            logging.info("performed operation of spark sql of shipment_age")
            return self.spark.sql(query_ship_age)

        except:
            print("error in query")
            logging.error("error occurred")

    def prod_vs_sales(self):
        try:
            data = self.spark.sql("select p.product_category_name,sum(py.payment_value) as product_sales"
                                  " from products p inner join items i on i.product_id=p.product_id"
                                  " inner join orders o on o.order_id=i.order_id inner join payments py"
                                  " on py.order_id=o.order_id group by p.product_category_name order by product_sales desc")

            logging.info("performed operation of spark sql of product vs sales 80 20")

            # dropping none type values
            data = data.na.drop(subset=["product_category_name"])
            return data

        except:
            print("error took place")

    def write(self, df_name, output_name):

        logging.info("writing into azure datalake")
        spark.conf.set("fs.azure.account.key.%s.blob.core.windows.net" % storage_account_name, storage_account_key)

        output_container_name = "output"
        output_container_path = "wasbs://%s@%s.blob.core.windows.net/result/%s" % (
            output_container_name, storage_account_name, output_name)
        output_blob_folder = "%s/wrangled_data_folder" % output_container_path

        (df_name.coalesce(1).write.mode("overwrite").option("header", "true").format("com.databricks.spark.csv").save(
            output_blob_folder))

    def read_create_views(self):
        try:
            reviews = self.spark.read.csv("/mnt/sparkdatastore/olist_order_reviews_dataset.csv", header=True,
                                          inferSchema=True)
            customers = self.spark.read.csv("/mnt/sparkdatastore/olist_customers_dataset.csv", header=True,
                                            inferSchema=True)
            geolocation = self.spark.read.csv("/mnt/sparkdatastore/olist_geolocation_dataset.csv", header=True,
                                              inferSchema=True)
            sellers = self.spark.read.csv("/mnt/sparkdatastore/olist_sellers_dataset.csv", header=True,
                                          inferSchema=True)
            orders = self.spark.read.csv("/mnt/sparkdatastore/olist_orders_dataset.csv", header=True, inferSchema=True)
            order_items = self.spark.read.csv("/mnt/sparkdatastore/olist_order_items_dataset.csv", header=True,
                                              inferSchema=True)
            products = self.spark.read.csv("/mnt/sparkdatastore/olist_products_dataset.csv", header=True,
                                           inferSchema=True)
            payments = self.spark.read.csv("/mnt/sparkdatastore/olist_order_payments_dataset.csv", header=True,
                                           inferSchema=True)
            products_translation = self.spark.read.csv("/mnt/sparkdatastore/product_category_name_translation.csv",
                                                       header=True, inferSchema=True)
            logging.info("creating temp views")

            # creating temp views to perform spark sql
            reviews.createOrReplaceTempView("reviews")
            customers.createOrReplaceTempView("customers")
            geolocation.createOrReplaceTempView("geolocation")
            sellers.createOrReplaceTempView("sellers")
            orders.createOrReplaceTempView("orders")
            order_items.createOrReplaceTempView("items")
            products.createOrReplaceTempView("products")
            payments.createOrReplaceTempView("payments")
            products_translation.createOrReplaceTempView("translation")

        except:
            print("unable to read and create views")
            logging.info("error occurred unable to display")


if __name__ == '__main__':
    logging.info("creating spark session")
    spark = SparkSession.builder.appName("spark").getOrCreate()

    storage_account_name = "storcapstone"
    storage_account_key = "4owdRw3ACe90/LLD8CWRVO1Jng7L44x/Qm/ApyPKWzy0TZzfp1gSDgrH+zDMYDYslLdnE/+KVF+VuLq5mLQhzQ=="
    container = "ecomdata"

    logging.info("Now we are going to mount data lake gen2 data")
    # reading data
    comm_data = ecomdata(storage_account_name, storage_account_key, container)
    comm_data.read_create_views()

    print("top selling products")
    top_sell = comm_data.top_selling_products()
    top_sell.show(15)
    comm_data.write(top_sell, "top_selling_products")

    print("revenue between particular year 2017-01-01 to 2018-01-01")
    start_date = input("enter start date in yyyy-mm-dd format:")
    end_date = input("enter end date in yyyy-mm-dd format:")
    total_rev = comm_data.total_revenue_years(start_date, end_date)
    total_rev.show()
    comm_data.write(total_rev, "revenue_years_between_dates")

    print("total orders by product category")
    orders_by_category = comm_data.orders_product_category()
    orders_by_category.show(10)
    comm_data.write(orders_by_category, "orders_by_category")

    print("Count total number of customers by regions")
    customers_total_reg = comm_data.No_customers_by_region()
    customers_total_reg.show(10)
    comm_data.write(customers_total_reg, "customers_by_region")

    print("total revenue generated per year")
    yearly_revenue = comm_data.rev_anually()
    yearly_revenue.show(10)
    comm_data.write(yearly_revenue, "yearly_revenue")

    print("most valued salesman")
    val_seller = comm_data.m_valued_salesman()
    val_seller.show()
    comm_data.write(val_seller, "val_sellers")

    print("Product sales over Time (scope of a particular product day by day)")
    sale_time = comm_data.prod_scop()
    sale_time.show(10)
    comm_data.write(sale_time, "sale_time_day_by_day")

    print("orders by region or city")
    orders_city = comm_data.orders_by_region_city()
    orders_city.show()
    comm_data.write(orders_city, "orders_by_region_city")

    print("most reviewed product")
    most_rev_prod = comm_data.reviewed_product()
    most_rev_prod.show(20)
    comm_data.write(most_rev_prod, "most_reviewed_prod")

    print("maximum and minimum priced products")
    max_price_prod, min_price_prod = comm_data.min_max_priced_products()
    min_price_prod.show()
    max_price_prod.show()
    comm_data.write(min_price_prod, "min_priced_products")
    comm_data.write(max_price_prod, "max_priced_products")

    print("loyal customers")
    val_cust = comm_data.m_valued_customer()
    val_cust.show()
    comm_data.write(val_cust, "loyal_valued_customers")

    print("orders placed by customers")
    plac_orders = comm_data.total_orders_customers()
    plac_orders.show(5)
    comm_data.write(plac_orders, "orders_placed_by_customers")

    print("shipment aging ")
    ship_age = comm_data.shipment_age()
    ship_age.show(5)
    comm_data.write(ship_age, "ship_aging")

    print("product vs sales")
    # b.pareto()
    prod_sales = comm_data.prod_vs_sales()
    prod_sales.show(5)
    comm_data.write(prod_sales, "product_sales_80_20")
