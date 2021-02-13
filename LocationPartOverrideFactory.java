/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   08 Jan 2008 17:50:44  $
   $Workfile:   LocationPartOverrideFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LocationPartOverrideFactory.java.-arc  $
/*
/*   Rev 1.2   08 Jan 2008 17:50:44   zf297a
/*Fixed logger
/*
/*   Rev 1.1   20 Dec 2007 00:07:04   zf297a
/*Use the new DBFactory constructor and report any SQLException.
/*
/*   Rev 1.0   Mar 08 2006 00:02:00   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class LocationPartOverrideFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(LocationPartOverrideFactory.class.getName());

     LocationPartOverrideFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
	     super(query, connection, prefetchValue) ;
	     logger.debug("query=" + query) ;
	     logger.debug("prefetchValue=" + prefetchValue) ;
     }
     LocationPartOverrideFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LocationPartOverride(rs) ;
        }
        catch (SQLException e) {
	    System.err.println("SQLException: " + e.getMessage()) ;
            return null ;
        }
    }
}
