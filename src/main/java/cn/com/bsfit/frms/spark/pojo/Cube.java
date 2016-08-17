package cn.com.bsfit.frms.spark.pojo;

import java.io.Serializable;

public class Cube implements Serializable {
	
	private int cube;
	private int value;
	private static final long serialVersionUID = 1L;
	
	public int getCube() {
		return cube;
	}
	public void setCube(int cube) {
		this.cube = cube;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "Cube [cube=" + cube + ", value=" + value + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cube;
		result = prime * result + value;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Cube other = (Cube) obj;
		if (cube != other.cube)
			return false;
		if (value != other.value)
			return false;
		return true;
	}
}
