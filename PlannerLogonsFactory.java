import java.sql.* ;
/*   $Author:   c970183  $
   $Revision:   1.1  $
       $Date:   Jun 15 2005 14:32:26  $
   $Workfile:   PlannerLogonsFactory.java  $
        $Log:   \\www-amssc-01\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PlannerLogonsFactory.java-arc  $
/*
/*   Rev 1.1   Jun 15 2005 14:32:26   c970183
/*Added PVCS keywords
	*/
public class PlannerLogonsFactory extends DBFactory
{
    PlannerLogonsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ; 
    }
    public Rec createRec() {
        try {
           return new PlannerLogons(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
