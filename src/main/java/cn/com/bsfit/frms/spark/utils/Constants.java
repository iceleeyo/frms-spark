package cn.com.bsfit.frms.spark.utils;

/**
 * 系统中的常量定义在这里
 * 
 * @author hjp
 * 
 * @since 1.0.0
 *
 */
public final class Constants {

	private Constants() {
		super();
	}

	/**
	 * spark app name
	 */
	public final static String APP_NAME = "FRMS_SPARK";

	/**
	 * jdbc url
	 */
	public final static String JDBC_URL = "jdbc:oracle:thin:@10.100.1.20:1521:db3";

	/**
	 * oracle driver class
	 */
	public final static String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";

	/**
	 * mysql driver class
	 */
	public final static String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
}
