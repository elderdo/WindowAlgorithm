/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   08 Jan 2008 22:56:48  $
   $Workfile:   RepairInvsSumFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\RepairInvsSumFactory.java.-arc  $
/*
/*   Rev 1.1   08 Jan 2008 22:56:48   zf297a
/*Fixed Logger
/*
/*   Rev 1.0   Oct 31 2005 21:38:48   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class RepairInvsSumFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(RepairInvsSumFactory.class.getName());

    RepairInvsSumFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new RepairInvsSum(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
