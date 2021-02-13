import java.sql.* ;
import org.apache.log4j.Logger ;

/*   $Author:   zf297a  $
   $Revision:   1.0  $
       $Date:   27 Oct 2008 10:14:56  $
   $Workfile:   BomDetailFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\BomDetailFactory.java.-arc  $
/*
/*   Rev 1.0   27 Oct 2008 10:14:56   zf297a
/*Initial revision.

	*/

public class BomDetailFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(BomDetailFactory.class.getName());

    BomDetailFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    BomDetailFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new BomDetail(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
