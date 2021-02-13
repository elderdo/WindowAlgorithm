import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpAttributeFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpAttributeFactory.class.getName());

    LpAttributeFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpAttributeFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpAttribute(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
