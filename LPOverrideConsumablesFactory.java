import java.sql.* ;
import org.apache.log4j.Logger ;
/*   $Author:   zf297a  $
   $Revision:   1.3  $
       $Date:   19 Dec 2007 14:18:28  $
   $Workfile:   LPOverrideConsumablesFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\LPOverrideConsumablesFactory.java.-arc  $
/*
/*   Rev 1.3   19 Dec 2007 14:18:28   zf297a
/*Added some logger.debug to make it easier in  tracing problems.  Added an error message to stderr whenever an SQLException occurs.
/*
/*   Rev 1.2   14 Dec 2007 21:17:04   zf297a
/*Fixed constructor
/*
/*   Rev 1.1   14 Dec 2007 20:59:14   zf297a
/*Add a new constructor that will utilize a query, a connection object, and a prefetch value.
/*
/*   Rev 1.0   18 Jul 2007 16:38:40   zf297a
/*Initial revision.
        */
public class LPOverrideConsumablesFactory extends DBFactory
{
	static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

	LPOverrideConsumablesFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LPOverrideConsumablesFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ; 
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LPOverrideConsumables(rs) ;
        }
        catch (SQLException e) {
	    System.err.println(e.getMessage()) ;
            return null ;
        }
    }   

}
