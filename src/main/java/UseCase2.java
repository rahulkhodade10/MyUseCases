import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;

public class UseCase2 {

    public static Dataset<Row> getUseCase2Result()
    {
        Dataset<Row> orders=UseCasesRowData.getOrders();
        Dataset<Row>customers=UseCasesRowData.getCustomers();
        Dataset<Row> DormantCustomers =orders.join(customers, orders.col("order_customer_id").equalTo(customers.col("customer_id")), "right_outer").
                where(orders.col("order_date").like("2014-01%").and(orders.col("order_customer_id").isNull())).select(customers.col("customer_id"),
                        col("customer_fname"), col("customer_lname"), col("customer_email"), col("customer_password"), col("customer_street"), col("customer_city"), col("customer_state"), col("customer_zipcode")).
                orderBy(customers.col("customer_id"));

        return DormantCustomers;
    }

    public static void main(String[] args) {

        String path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\outputs\\useCase2";
        getUseCase2Result().coalesce(1).write().option("header",true).mode("overwrite").csv(path);

    }

}
