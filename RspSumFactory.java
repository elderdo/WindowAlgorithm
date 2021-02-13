/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   31 Jan 2008 13:47:48  $
   $Workfile:   RspSumFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\RspSumFactory.java.-arc  $
/*
/*   Rev 1.1   31 Jan 2008 13:47:48   zf297a
/*fixed syntax error
/*
/*   Rev 1.0   31 Jan 2008 10:54:46   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class RspSumFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(RspSumFactory.class.getName());

    RspSumFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
	    super(query, connection, prefetchValue) ;
	    logger.debug("query=" + query) ;
	    logger.debug("prefetch=" + prefetchValue) ;
    }

    RspSumFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new RspSum(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
