import java.sql.*;
import java.io.*;
import java.util.Properties;

/*
$Author:   zf297a  $
$Revision:   1.2  $
$Date:   14 Dec 2007 20:20:48  $
$Workfile:   DBConnection.java  $
$Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\DBConnection.java.-arc  $
/*
/*   Rev 1.2   14 Dec 2007 20:20:48   zf297a
/*Fixed setIniFile to use createConnection and fixed the default constructor to check for good connection arguments.
/*
/*   Rev 1.1   14 Dec 2007 13:35:36   zf297a
/*make loadParams protected
/*
/*   Rev 1.0   14 Dec 2007 10:11:44   zf297a
/*Initial revision.

**/

public class DBConnection {
    public Connection c;

    private String dbUrl;
    private String uid;
    private String pwd;


    public void setConnectionStringProperty(String value) {
        connectionStringProperty = value;
    }

    public void setUidProperty(String value) {
        uidProperty = value;
    }

    public void setPwdProperty(String value) {
        pwdProperty = value;
    }

    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    protected Properties dbConnectionProperties = new Properties();
    private String iniFile;
    private String propertiesFile = "DBConnection.properties";

    private String connectionStringProperty = "ConnectionString";
    private String uidProperty = "UID";
    private String pwdProperty = "PWD";

    // Using the classpath load properties and using
    // the following property names: ConnectionString, UID, and PWD
    // load those variables into dbUrl, uid, and pwd
    protected void loadParams() throws Exception, java.io.IOException {
        // properties via classpath
        java.net.URL url = ClassLoader.getSystemResource(propertiesFile);
        if (url == null) {
            throw new Exception("Cannot find " + propertiesFile);
        } else {
            dbConnectionProperties.load(url.openStream());
            dbUrl = dbConnectionProperties.getProperty(connectionStringProperty);
            uid = dbConnectionProperties.getProperty(uidProperty);
            pwd = dbConnectionProperties.getProperty(pwdProperty);
        }
    }

    // Using a file name load properties and using
    // the following property names: ConnectionString, UID, and PWD
    // load those variables into dbUrl, uid, and pwd
    public void loadParams(String propertiesFile) throws Exception {
        // properties via a file
        if (propertiesFile != null && !propertiesFile.equals("")) {
            File f = new File(propertiesFile);
            if (!f.exists()) {
                throw new Exception(f + " does not exist");
            }
            try {
                FileInputStream fis = new FileInputStream(f);
                dbConnectionProperties.load(fis);
                fis.close();
                dbUrl = dbConnectionProperties.getProperty(connectionStringProperty);
                uid = dbConnectionProperties.getProperty(uidProperty);
                pwd = dbConnectionProperties.getProperty(pwdProperty);
            } catch (Exception e) {
                throw new Exception("Unable to process file: " + propertiesFile + " " + e.getMessage());
            }
        } else {
            throw new Exception("Invalid properties file.");
        }
    }

    // Using the Oracle jdbc driver
    // create a public connection object c via dbUrl, uid, and pwd
    public void createConnection() throws java.lang.ClassNotFoundException,
        java.sql.SQLException, Exception {

            Class.forName("oracle.jdbc.driver.OracleDriver");
            if (dbUrl != null && uid != null && pwd != null) {
                c = DriverManager.getConnection(dbUrl, uid, pwd);
            } else {
                throw new Exception("Connection parameters not set.");
            }
        }

    // Try to set up the connection with an ini file which
    // is identical to a properties file.
    // If the connection cannot be made, abort the application.
    public void setIniFile(String iniFile) {

        this.iniFile = iniFile;

        try {

            loadParams(iniFile);

            if (c != null) {
                c.close();
            }
            createConnection();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            System.exit(4);
        }
    }

    public String getIniFile() {
        return iniFile;
    }

    // Create a connection using the name of the propertiesFile that gets loaded via the classpath,
    // the property name of the dbUrl, the property name of the user id, and the property name of the password.
    // For example "MyConnection.properties","myDBurl","myUid", and "myPwd" which looks for a file in the classpath
    // named MyConnection.properties containing
    // myDBurl = some connection string
    // myUid = some user id
    // myPwd = the password for myUid
    public DBConnection(String propertiesFile, String dbUrlProperty, String uidProperty, String pwdProperty) {

        this.propertiesFile = propertiesFile;

        connectionStringProperty = dbUrlProperty;
        this.uidProperty = uidProperty;
        this.pwdProperty = pwdProperty;

        try {
            loadParams();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }

        dbUrl = dbConnectionProperties.getProperty(dbUrlProperty);
        uid = dbConnectionProperties.getProperty(uidProperty);
        pwd = dbConnectionProperties.getProperty(pwdProperty);

        try {
            createConnection();
        } catch (java.lang.ClassNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(16);
        } catch (java.sql.SQLException e) {
            System.err.println(e.getMessage());
            System.exit(8);
        } catch (java.lang.Exception e) {
            System.err.println(e.getMessage());
            System.exit(4);

        }
    }



    // construct the connection using an explicit database url, user id, and password
    public DBConnection(String dbUrl, String uid, String pwd) throws java.lang.ClassNotFoundException, java.sql.SQLException, java.lang.Exception {
        this.dbUrl = dbUrl;
        this.uid = uid;
        this.pwd = pwd;
        createConnection();
    }

    public DBConnection(String propertiesFile) throws Exception {
        this.propertiesFile = propertiesFile;
        loadParams();
        if (dbUrl != null && uid != null && pwd != null) {
            createConnection();
        } else {
            throw new Exception("Properties file " + propertiesFile + " does not contain keywords for dbUrl, uid, and pwd");
        }

    }

    // Use the DBConnection.properties to connect
    public DBConnection() throws SQLException, ClassNotFoundException, Exception {

            loadParams();
            if (dbUrl != null && uid != null && pwd != null) {
                createConnection();
            }

    }


}

