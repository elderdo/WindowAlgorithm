import java.sql.* ;
import org.apache.log4j.Logger ;

public class BomLocationContractFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(BomLocationContractFactory.class.getName());

    BomLocationContractFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    BomLocationContractFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new BomLocationContract(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
