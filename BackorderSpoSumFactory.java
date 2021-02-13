import java.sql.* ;
import org.apache.log4j.Logger ;

/*   $Author:   zf297a  $
   $Revision:   1.0  $
       $Date:   Jun 28 2006 13:17:18  $
   $Workfile:   BackorderSpoSumFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\BackorderSpoSumFactory.java.-arc  $
/*
/*   Rev 1.0   Jun 28 2006 13:17:18   zf297a
/*Initial revision.
*/

public class BackorderSpoSumFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    BackorderSpoSumFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new BackorderSpoSum(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
