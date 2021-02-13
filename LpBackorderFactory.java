import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpBackorderFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpBackorderFactory.class.getName());

    LpBackorderFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpBackorderFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpBackorder(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
