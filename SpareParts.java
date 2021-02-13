import java.sql.* ;

public class SpareParts extends TmpSpareParts
{
        SpareParts() throws SQLException {
            AmdConnection amd = AmdConnection.instance() ;
            Statement s = amd.c.createStatement() ;
            rs = s.executeQuery("Select * from amd_spare_parts order by part_no, mfgr") ;
        }

}