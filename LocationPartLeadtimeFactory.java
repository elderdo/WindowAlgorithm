/*   $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   18 Nov 2008 14:46:38  $
   $Workfile:   LocationPartLeadtimeFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LocationPartLeadtimeFactory.java.-arc  $
/*
/*   Rev 1.1   18 Nov 2008 14:46:38   zf297a
/*Add constructor that takes the connection object and a prefetch value.
/*
/*   Rev 1.0   Mar 08 2006 00:00:54   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class LocationPartLeadtimeFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    LocationPartLeadtimeFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

     LocationPartLeadtimeFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new LocationPartLeadtime(rs) ;
        }
        catch (SQLException e) {
	    logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
