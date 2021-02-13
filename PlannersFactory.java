import java.sql.* ;
/*   $Author:   c970183  $
   $Revision:   1.1  $
       $Date:   Jun 15 2005 14:32:24  $
   $Workfile:   PlannersFactory.java  $
        $Log:   \\www-amssc-01\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PlannersFactory.java-arc  $
/*
/*   Rev 1.1   Jun 15 2005 14:32:24   c970183
/*Added PVCS keywords
	*/
public class PlannersFactory extends DBFactory
{
    PlannersFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ; 
    }
    public Rec createRec() {
        try {
           return new Planner(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
