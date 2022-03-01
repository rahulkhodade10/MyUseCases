import org.junit.Assert;
import org.junit.Test;

public class TestUseCase3 {

    public static long getOrderCount()
    {
        long orderCount=UseCasesRowData.getOrders().count();
        return orderCount;
    }
    public static long getOrder_items_Count()
    {
        long order_items_count=UseCasesRowData.getOrder_items().count();
        return order_items_count;
    }
    public static long getCustomerCount()
    {
        long customerCount=UseCasesRowData.getCustomers().count();
        return customerCount;
    }
    public static long getResultCount()
    {
        long resultCount=UseCase3.getUseCase3Result().count();
        return resultCount;
    }

    @Test

    public void validateOrders()
    {
        Assert.assertEquals(68883,getOrderCount());
    }

    @Test

    public void validateOrder_Items()
    {
        Assert.assertEquals(172198,getOrder_items_Count());
    }
    @Test
    public void validateCustomers()
    {
        Assert.assertEquals(12435,getCustomerCount());
    }
    @Test
    public void validateResult()
    {
        Assert.assertEquals(1941,getResultCount());
    }

}
