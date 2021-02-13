import java.sql.* ;
public class TestSortMerge
{
    public static void main(String[] args) {
        SortMerge sm = new SortMerge() ;
        try {
            SpareParts F1 = new SpareParts() ;
            TmpSpareParts F2 = new TmpSpareParts() ;
            sm.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
        }
    }
}
