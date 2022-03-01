import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Assert;
import org.junit.Test;

public class TestUseCase5 {

    public static long getDepartmentCount()
    {
        long departmentCount=UseCasesRowData.getDepartments().count();
        return departmentCount;
    }

    public static long getProductCount()
    {
        long productCount=UseCasesRowData.getProducts().count();
        return productCount;
    }

    public static long getCategories()
    {
        long categoriesCount=UseCasesRowData.getCategories().count();
        return categoriesCount;
    }

    public static long getResultCount()
    {
        long resultCount=UseCase5.getUseCase5Result().count();
        return resultCount;
    }

    @Test
    public void validateDepartment()
    {
        Assert.assertEquals(6,getDepartmentCount());
    }

    @Test

    public void validateProduct()
    {
        Assert.assertEquals(1345,getProductCount());

    }

    @Test

    public void validateCategories()
    {
        Assert.assertEquals(58,getCategories());
    }

    @Test

    public void validResult()
    {
        Assert.assertEquals(6,getResultCount());
    }
}
