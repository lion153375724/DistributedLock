package com.example.demo;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.example.demo.codis.JodisPoolFactory;
import com.example.demo.codis.JodisTemplate;
import com.example.demo.lock.DistributedLock;

@Component
public class RedisDistributedLock extends DistributedLock{
	
	private JodisTemplate jodisTemplate = JodisPoolFactory.getJodisTemplate();
	
	/**
	 * lock key
	 */
	private String key;
	
	/**
	 * 超时时间
	 */
	private int timeout;

	/**
	 * 是否快速试错
	 */
	private boolean fastFail;
	
	/**
     * 版本号时间，作为获取锁的客户端操作的依据
     */
    private long versionTime;
    
	public RedisDistributedLock(){}
	
	/**
	 * 默认fastFail为false,需要再次竟争
	 * @param key
	 * @param timeout
	 */
	public RedisDistributedLock(String key,int timeout){
		this(key,timeout,false);
	}
	
	public RedisDistributedLock(String key,int timeout,boolean fastFail){
		this.key = key;
		this.timeout = timeout;
		this.fastFail = fastFail;
	}
	
	@Override
	public boolean lock(){
		//1、setnx试图获取锁
		boolean setnx = jodisTemplate.setnx(key, buildVal(), timeout);
		//如果 sexnx获取成，成功获取了锁，如果setnx失败并且fastFail(快速试错)为true,直接返回false
		if(setnx || fastFail){
			return setnx;
		}else{
			//setnx失败并且fastFail(快速试错)为false
			//说明锁被其它进程占领着,检查其是否超时，未超时，直接返回锁竞争失败
			Long oldVal = getLong(jodisTemplate.get(key));
			
			//System.out.println(oldVal+":"+System.currentTimeMillis());
			//System.out.println(oldVal > System.currentTimeMillis());
			//未超时
			if(oldVal > System.currentTimeMillis()){
				return false;
			}else{
				//已超时,重新竞争锁，防止死锁，(expired未生效)
				Long getsetVal = getLong(jodisTemplate.getSet(key, buildVal()));
				jodisTemplate.expire(key, timeout);
				//如果相同，说明是同一个,竞争锁成功
				if(getsetVal == oldVal){
					return true;
				}else{
					//如果不同，说明锁已被其它进程抢走
					return false;
				}
			}
		}
	}
 
	@Override
	public boolean unLock() {
		System.out.println("unlock:++++++++++++++++++++++"+ Thread.currentThread().getName());
		long result = jodisTemplate.del(key);
		if(result == 1 ){
			return true;
		}
		return false;
	}
	
	@Override
	public boolean check() {
		long val = getLong(jodisTemplate.get(key));
		return versionTime == val;
	}
	
	private long getLong(String value){
		return StringUtils.isBlank(value) ? 0 : Long.valueOf(value);
	}
	/**
	 * 生成key的val
	 * @return
	 */
	private String buildVal(){
		versionTime = System.currentTimeMillis() + timeout*1000 +1;
		return String.valueOf(versionTime);
	}
	
	/**
	 * 获取当前系统生成key的val
	 * @return
	 */
	public String getVal(){
		long time = System.currentTimeMillis() + timeout*1000 +1;
		return String.valueOf(time);
	}

}
