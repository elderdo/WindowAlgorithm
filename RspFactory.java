/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   31 Jan 2008 12:22:08  $
   $Workfile:   RspFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\RspFactory.java.-arc  $
/*
/*   Rev 1.2   31 Jan 2008 12:22:08   zf297a
/*Create a new constructor using query, DBConnection and a prefetch value.
/*
/*   Rev 1.1   08 Jan 2008 22:57:18   zf297a
/*Fixed Logger
/*
/*   Rev 1.0   Jun 05 2006 09:32:08   zf297a
/*Initial revision.
/*
/*   Rev 1.0   Jul 30 2004 12:29:36   c970183
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class RspFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(RspFactory.class.getName());

    RspFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ;
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    RspFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new Rsp(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
