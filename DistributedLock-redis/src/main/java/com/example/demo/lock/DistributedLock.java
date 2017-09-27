package com.example.demo.lock;


/**
 * 抽象锁
 * @author Administrator
 *
 */
public abstract class DistributedLock {
	
	/**
	 * 获取锁
	 * @return
	 */
	public boolean tryLock(){
		try {
			return this.lock();
		} catch (Exception e) {
			return false;
		}
	}
	
	/**
	 * 获取锁
	 * @return
	 */
	public abstract boolean lock();
	
	/**
	 * 检测锁是否存在，是否有效，是否被其它进程占用
	 * @return
	 */
	public abstract boolean check();
	
	/**
	 * 释放锁
	 * @return
	 */
	public abstract boolean unLock();
}
