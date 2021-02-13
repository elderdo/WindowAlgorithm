import java.sql.* ;
/*   $Author:   c970183  $
   $Revision:   1.7  $
       $Date:   21 Apr 2002 12:35:24  $
   $Workfile:   TestAlgo.java  $
*/
/* another change */
public class TestAlgo
{
    static private String revision = "$Revision:   1.7  $" ;
    static private String author = "$Author:   c970183  $" ;
    static private String workfile = "$Workfile:   TestAlgo.java  $" ;
    
    public String getRevision() {
        return revision ;
    }
    
    public String getAuthor() {
        return author ;
    }
    
    public String getWorkfile() {
        return workfile ;
    }
    static private String pvcsInfo (String args[]) {
    	int i = 0 ;
	    String arg ;
	    while (i < args.length) {
	        arg = args[i++] ;
	        if (arg.equals("-version")) {
	            return revision + "\n"
	            + author + "\n"
	            + workfile ;
	        }
	    }
	    return "" ;
    }
    
    public static void main(String[] args) {
        
        String info = pvcsInfo(args) ;
        if (!info.equals("")) {
            System.out.println(info) ;
            System.exit(0) ;
        }
        
        WindowAlgo w = new WindowAlgo() ;
        try {
            SpareParts F1 = new SpareParts() ;
            TmpSpareParts F2 = new TmpSpareParts() ;
            w.diff(F1, F2) ;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage()) ;
        }
    }
}
/* Simulate a FILE data source */
class MyFileSnapshot implements Snapshot {
    InputBuffer buf = new InputBuffer() ;
    int cnt = 0 ;
    MyFileSnapshot() {
        MyRec r[] = new MyRec[5];
        buf.add(new MyRec("1","ABC")) ;
        buf.add(new MyRec("2","DEF") ) ;
        buf.add(new MyRec("3","GHI") ) ;
        buf.add(new MyRec("4","JKL")) ;
        buf.add(new MyRec("5","MNO") ) ;
    }
    public void save() {
    }
    public InputBuffer getBlocks(int n) {
        if (cnt == 0) {
            cnt++ ;
            return buf ;
        }
        else
            return null ;
    }
}
/* Simulate a Table data source */
class MyTableSnapshot implements Snapshot {
    InputBuffer buf = new InputBuffer() ;
    int cnt = 0 ;
    MyTableSnapshot() {
        MyRec r[] = new MyRec[5];
        buf.add(new MyRec("1","ABX")) ;
        buf.add(new MyRec("3","GHI") ) ;
        buf.add(new MyRec("4","JKL")) ;
        buf.add(new MyRec("5","MNO") ) ;
        buf.add(new MyRec("6","PQR") ) ;
    }
    public void save() {
        System.out.println("Saving new baseline SNAPSHOT to test.dat") ;
    }
    public InputBuffer getBlocks(int n) {
        if (cnt == 0) {
            cnt++ ;
            return buf ;
        }
        else
            return null ;
    }
}
class MyRec implements Rec {
    String key ;
    String body ;

    MyRec(String key, String body) {
        this.key = key ;
        this.body = body ;
    }
    
    public Comparable  getKey() {
        return  key ;
    }
    public Object getBody() {
        return body ;
    }
    
    public boolean  keysEqual(Rec r) {
        return (key.equals( r.getKey())) ;
    }
    public boolean  bodiesEqual(Rec r) {
        return (body.equals(r.getBody())) ;
    }
    public void     insert() {
        System.out.println("Insert: key=" + key + " body=" + body) ;
    }

    public void     update() {
        System.out.println("Update: key=" + key + " body=" + body) ;
    }
    public void     delete() {
        System.out.println("Delete: key=" + key) ;
    }
}