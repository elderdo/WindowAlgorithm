import java.sql.* ;

public class Movies extends TmpMovies
{
        Movies() throws SQLException, ClassNotFoundException {
            AmdConnection amd = new AmdConnection() ;
            Statement s = amd.c.createStatement() ;
            rs = s.executeQuery("Select * from movie order by id desc") ;
        }
}
