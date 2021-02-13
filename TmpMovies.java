import java.sql.* ;

public class TmpMovies implements SortedSnapshot {
    final int MAXBUF = 73 ;
    protected InputBuffer buf = new InputBuffer(MAXBUF) ;
    protected ResultSet rs ;
    
    TmpMovies() throws SQLException, ClassNotFoundException {
        AmdConnection amd = new AmdConnection() ;
        Statement s = amd.c.createStatement() ;
        s.setFetchSize(100) ;
        rs = s.executeQuery("Select * from movie2");
    }
    public void save() {
    }
    public Rec read() {
        try {
            if (rs.next())
                return new Movie(rs) ;
            else
                return null ;
        }
        catch (SQLException e) {
            return null ;
        }
    }
    public InputBuffer getBlocks(int n) {
        try {
            for (int i = 0; (i < n && rs.next()); i++) {
		    Rec theRec = new Movie(rs) ;
                buf.put(theRec.getKey(),theRec) ;
            }
            if (buf.size() > 0) 
                return buf ;
            else
                return null ;
        }
        catch (SQLException e) {
            if (buf.size() > 0) 
                return buf ;
            else
                return null ;
        }
    }
}
