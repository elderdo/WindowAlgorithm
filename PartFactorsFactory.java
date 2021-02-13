/*   $Author:   zf297a  $
   $Revision:   1.2  $
       $Date:   12 Aug 2008 13:29:56  $
   $Workfile:   PartFactorsFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PartFactorsFactory.java.-arc  $
/*
/*   Rev 1.2   12 Aug 2008 13:29:56   zf297a
/*Added contructor that takes the connection object and a prefetch value in addition to the sql query string.
/*
/*   Rev 1.1   08 Jan 2008 22:55:48   zf297a
/*Fixed Logger
/*
/*   Rev 1.0   Mar 07 2006 23:55:54   zf297a
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class  PartFactorsFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(PartFactorsFactory.class.getName());

    PartFactorsFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    PartFactorsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new PartFactors(rs) ;
        }
        catch (SQLException e) {
	    logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}
