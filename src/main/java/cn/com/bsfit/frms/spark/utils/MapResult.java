package cn.com.bsfit.frms.spark.utils;

import java.io.Serializable;
import java.sql.ResultSet;

import org.apache.spark.rdd.JdbcRDD;

import scala.runtime.AbstractFunction1;

public class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public Object[] apply(final ResultSet resultSet) {
		return JdbcRDD.resultSetToObjectArray(resultSet);
	}

}
