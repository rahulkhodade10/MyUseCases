import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;


public class UseCase3 {

    public static Dataset<Row> getUseCase3Result()
    {
        Dataset<Row> orders=UseCasesRowData.getOrders();
        Dataset<Row> customers=UseCasesRowData.getCustomers();
        Dataset<Row> order_items=UseCasesRowData.getOrder_items();



        Dataset<Row> join1 = orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id")), "right_outer");

        Dataset<Row> result3 = join1.join(order_items, join1.col("order_id").equalTo(order_items.col("order_item_order_id"))).
                where(orders.col("order_date").like("2014-01%").and(join1.col("order_status").isin("COMPLETE", "CLOSED"))).
                groupBy(join1.col("customer_id"),
                        join1.col("customer_fname").alias("customer_first_name"),
                        join1.col("customer_lname").alias("customer_last_name")).
                agg(coalesce(round(sum(order_items.col("order_item_subtotal")), 2), lit(0)).alias("customer_revenue")).
                orderBy(col("customer_revenue").desc(), join1.col("customer_id"));
        return result3;


    }

    public static void main(String[] args) {

        String path="C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\outputs\\useCase3";
        getUseCase3Result().coalesce(1).write().option("header",true).mode("overwrite").csv(path);



    }

}
