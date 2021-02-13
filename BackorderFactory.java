import java.sql.* ;
import org.apache.log4j.Logger ;

/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   15 Jan 2009 23:56:46  $
   $Workfile:   BackorderFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\BackorderFactory.java.-arc  $
/*
/*   Rev 1.1   15 Jan 2009 23:56:46   zf297a
/*Added a general exception handler to the createRec method.
/*
/*   Rev 1.0   Dec 07 2005 14:36:12   zf297a
/*Initial revision.
*/

public class BackorderFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    BackorderFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new Backorder(rs) ;
        }
        catch (SQLException e) {
		logger.debug("SQLException " + e.getMessage()) ;
            	return null ;
        }
        catch (Exception e) {
		logger.debug("General Exception: " + e.getMessage()) ;
		return null ;
	}
    }
}
