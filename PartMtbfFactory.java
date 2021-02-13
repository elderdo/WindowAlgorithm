import java.sql.* ;
import org.apache.log4j.Logger ;

public class PartMtbfFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(PartMtbfFactory.class.getName());

    PartMtbfFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    PartMtbfFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new PartMtbf(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
