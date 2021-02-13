import java.sql.* ;
import org.apache.log4j.Logger ;

/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   31 Jan 2008 12:23:52  $
   $Workfile:   AmdReqsFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\AmdReqsFactory.java.-arc  $
/*
/*   Rev 1.1   31 Jan 2008 12:23:52   zf297a
/*Added constructor using query, DBConnection and prefetch value.
/*
/*   Rev 1.0   Dec 07 2005 14:33:40   zf297a
/*Initial revision.
*/

public class AmdReqsFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(AmdReqsFactory.class.getName());

    AmdReqsFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ;
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    AmdReqsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
	logger.debug("query=" + query) ;
    }

    public Rec createRec() {
        try {
            return new AmdReqs(rs) ;
        }
        catch (SQLException e) {
	    System.err.println(e.getMessage()) ;
            return null ;
        }
    }
}
