/*  
     $Author:   zf297a  $
   $Revision:   1.1  $
       $Date:   08 Jan 2008 18:05:18  $
   $Workfile:   AppProperties.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\AppProperties.java.-arc  $
/*
/*   Rev 1.1   08 Jan 2008 18:05:18   zf297a
/*fixed pathname for the properties file
/*
/*   Rev 1.0   19 Dec 2007 23:55:58   zf297a
/*Initial revision.
**/

public class AppProperties  {
	
	private final String DEFAULT_FILE = "default.properties" ;
	private final String APP_FILE = "application.properties" ;

	private java.util.Properties defaults = new java.util.Properties() ;
	private java.util.Properties p = new java.util.Properties(defaults) ;

	public java.util.Properties getDefaults() {
		return defaults ;
	}

	public java.util.Properties getProperties() {
		return p ;
	}

	public void loadProperties(String filename, java.util.Properties p) throws java.io.IOException, java.lang.Exception {

		// properties via classpath
    		java.net.URL url = ClassLoader.getSystemResource(filename);
		if (url != null) {
			p.load(url.openStream());
		} else {
			throw new Exception("Unable to load file " + filename + " via the classpath.") ;
		}
	}

	public AppProperties() throws java.io.IOException, java.lang.Exception {
		loadProperties(DEFAULT_FILE, defaults) ;
		loadProperties(APP_FILE, p) ;
	}

	public AppProperties(String classname, String defaultName) throws java.io.IOException, java.lang.Exception {
		loadProperties(defaultName + ".properties", defaults) ;
		loadProperties(classname + ".properties", p) ;
	}

	public AppProperties(String classname) throws java.io.IOException, java.lang.Exception {
		loadProperties(DEFAULT_FILE, defaults) ;
		loadProperties(classname + ".properties", p) ;
	}

}
