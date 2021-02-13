import java.sql.* ;
public class UsersFactory extends DBFactory
{

    UsersFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
	super(query, connection, prefetchValue) ; 
    }
    UsersFactory(String query) throws ClassNotFoundException, SQLException {
	super(query) ; 
    }
    public Rec createRec() {
        try {
            return new Users(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
