/*   $Author:   zf297a  $
   $Revision:   1.0  $
       $Date:   Sep 26 2005 11:47:32  $
   $Workfile:   OnHandInvsSumFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\OnHandInvsSumFactory.java-arc  $
/*
/*   Rev 1.0   Sep 26 2005 11:47:32   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class OnHandInvsSumFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    OnHandInvsSumFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new OnHandInvsSum(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
