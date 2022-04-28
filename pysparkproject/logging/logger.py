import logging
from jobs import etl_jobs as job


def main():
    logging.basicConfig(filename='D:\sparklogs.log', level=logging.INFO)
    logging.info('Started')
    comm_data = job.ecomdata()
    comm_data.read_create_views()

    print("top selling products")
    top_sell = comm_data.top_selling_products()
    top_sell.show(15)

    print("revenue between particular year 2017-01-01 to 2018-01-01")
    start_date = input("enter start date in yyyy-mm-dd format:")
    end_date = input("enter end date in yyyy-mm-dd format:")
    total_rev = comm_data.total_revenue_years(start_date, end_date)
    total_rev.show()

    print("total orders by product category")
    orders_by_category = comm_data.orders_product_category()
    orders_by_category.show(10)

    print("Count total number of customers by regions")
    customers_total = comm_data.No_customers_by_region()
    customers_total.show(10)

    print("total revenue generated per year")
    yearly_revenue = comm_data.rev_anually()
    yearly_revenue.show(10)

    print("most valued salesman")
    val_seller = comm_data.m_valued_salesman()
    val_seller.show()

    print("Product sales over Time (scope of a particular product day by day)")
    sale_time = comm_data.prod_scop()
    sale_time.show(10)

    print("orders by region or city")
    orders_city = comm_data.orders_by_region_city()
    orders_city.show()

    print("most reviewed product")
    most_rev_prod = comm_data.reviewed_product()
    most_rev_prod.show(10)

    print("maximum and minimum priced products")
    max_price, min_price = comm_data.min_max_priced_products()
    max_price.show()
    min_price.show()

    print("loyal customers")
    val_cust = comm_data.m_valued_customer()
    val_cust.show()

    print("orders placed by customers")
    placed_orders = comm_data.total_orders_customers()
    placed_orders.show(5)

    print("shipment aging ")
    ship = comm_data.shipment_age()
    ship.show(5)

    print("product vs sales")
    prod_sales = comm_data.prod_vs_sales()
    prod_sales.show(5)

    logging.info('Finished')


if __name__ == '__main__':
    main()
