package com.example.demo.codis;

import java.util.HashMap;
import java.util.Map;

/** 结果集map。
 *
 */
public class ResultMap {
	private Map<String, Object> holder;

	public ResultMap() {
		this.holder = new HashMap<String, Object>();
	}
	
	/**暴露数据
	 * @param key
	 * @param val
	 */
	public void export(String key, Object val) {
		holder.put(key, val);
	}
	
	/**获取数据
	 * @param key
	 */
	@SuppressWarnings("unchecked")
	public  <T> T getByKey(String key) {
		return (T) holder.get(key);
	}
	
	/** 数据个数统计
	 * @return
	 */
	public int size() {
		return holder.size();
	}
	
	
	
}
