import org.junit.Assert;
import org.junit.Test;

public class TestUseCase2 {

    public static long getOrderCount()
    {
        long orderCount=UseCasesRowData.getOrders().count();
        return orderCount;
    }

    public static long getCustomerCount()
    {
        long customerCount=UseCasesRowData.getCustomers().count();
        return customerCount;
    }

    public static long getResultCount()
    {
        long resultCount=UseCase2.getUseCase2Result().count();
        return resultCount;
    }

    @Test

    public void validateOrders()
    {
        Assert.assertEquals(68883,getOrderCount());
    }
    @Test

    public void validateCustomers()
    {
        Assert.assertEquals(12435,getCustomerCount());
    }

    @Test

    public void validateResult()
    {
        Assert.assertEquals(0,getResultCount());
    }

}