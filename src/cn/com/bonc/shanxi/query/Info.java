package cn.com.bonc.shanxi.query;

public class Info {
	public Info() {
	}
	int number = 0;
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}
	
	public synchronized void add(){
		number++;
	}
	
	boolean bool = false;
	
	public boolean getBoolean() {
		return bool;
	}
	
	public void setBoolean(boolean b) {
		this.bool = b;
	}
}
