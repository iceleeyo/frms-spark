package cn.com.bsfit.frms.spark.utils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.runtime.AbstractFunction0;

public class DbConnection extends AbstractFunction0<Connection>implements Serializable {

	private final String userName;
	private final String password;
	private final String connectionUrl;
	private final String driverClassName;
	private static final long serialVersionUID = 1L;
	private final static Logger LOGGER = LoggerFactory.getLogger(DbConnection.class);

	public DbConnection(final String userName, final String password, final String connectionUrl,
			final String driverClassName) {
		this.driverClassName = driverClassName;
		this.connectionUrl = connectionUrl;
		this.userName = userName;
		this.password = password;
	}

	@Override
	public Connection apply() {
		try {
			Class.forName(driverClassName);
		} catch (ClassNotFoundException e) {
			LOGGER.error("Failed to load driver class", e);
		}
		
		final Properties properties = new Properties();
		properties.setProperty("user", userName);
		properties.setProperty("password", password);

		Connection connection = null;
		try {
			connection = DriverManager.getConnection(connectionUrl, properties);
		} catch (SQLException e) {
			LOGGER.error("Connection failed", e);
		}

		return connection;
	}

}
