package cn.com.bsfit.frms.spark.pojo;

import java.io.Serializable;

public class Square implements Serializable {
	
	private int value;
	private int square;
	private static final long serialVersionUID = 1L;
	
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	public int getSquare() {
		return square;
	}
	public void setSquare(int square) {
		this.square = square;
	}
	@Override
	public String toString() {
		return "Square [value=" + value + ", square=" + square + "]";
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + square;
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
		Square other = (Square) obj;
		if (square != other.square)
			return false;
		if (value != other.value)
			return false;
		return true;
	}
}
