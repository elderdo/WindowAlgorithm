/*   $Author:   c970183  $
   $Revision:   1.0  $
       $Date:   Jul 30 2004 12:29:36  $
   $Workfile:   OnHandInvsFactory.java  $
        $Log:   \\www-amssc-01\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\OnHandInvsFactory.java-arc  $
/*
/*   Rev 1.0   Jul 30 2004 12:29:36   c970183
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class OnHandInvsFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    OnHandInvsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new OnHandInvs(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}