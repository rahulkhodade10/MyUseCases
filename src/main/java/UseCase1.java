import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class UseCase1 {

    public static Dataset<Row> getUseCase1Result()
{
    Dataset<Row> orders=UseCasesRowData.getOrders();
    Dataset<Row>customers=UseCasesRowData.getCustomers();
    Dataset<Row> CustomerOrderCount = orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id"))).
            where(orders.col("order_date").like("2014-01%")).
            groupBy(customers.col("customer_id"),
                    customers.col("customer_fname"),
                    customers.col("customer_lname")).
            agg(count(orders.col("order_customer_id")).alias("customer_order_count")).
            orderBy(col("customer_order_count").desc(), customers.col("customer_id"));

    return CustomerOrderCount;

}


    public static void main(String[] args) {


        String path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\outputs\\useCase1";
        getUseCase1Result().coalesce(1).write().option("header", true).mode("overwrite").csv(path);


    }
}