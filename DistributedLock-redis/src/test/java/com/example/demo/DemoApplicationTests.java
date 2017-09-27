package com.example.demo;




public class DemoApplicationTests {
	public static void main(String[] args) {
		/*JodisTemplate jodisTemplate = JodisPoolFactory.getJodisTemplate();
		System.out.println(jodisTemplate.get("lock-key-1"));
		jodisTemplate.setnx("test-111", "test", 30);*/
		
		for(int i=0;i<5;i++){
    		Thread thread = new Thread(new Runnable() {
				public void run() {
					RedisDistributedLock lock = new RedisDistributedLock("lock-key-1", 30);
					if (lock.tryLock()) {
			        	System.out.println("lock:++++++++++++++++++++++"+ Thread.currentThread().getName());
			            // 业务逻辑
			        	try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}//获得锁之后可以进行相应的处理
			            System.out.println("======获得锁后进行相应的操作======");
			            // 业务执行完毕 解锁
			            lock.unLock();
			            
			        }
				}
			});
    		
    		thread.start();
    	}
	}
	
	public static void testLock(){
		for(int i=0;i<1;i++){
    		Thread thread = new Thread(new Runnable() {
				public void run() {
					try{
						RedisDistributedLock lock = new RedisDistributedLock("lock-key-1", 30);
				        // 获取锁成功，执行业务
				        if (lock.tryLock()) {
				        	System.out.println("lock:++++++++++++++++++++++"+ Thread.currentThread().getName());
				            // 业务逻辑
				        	Thread.sleep(1000);//获得锁之后可以进行相应的处理
				            System.out.println("======获得锁后进行相应的操作======");
				            // 业务执行完毕 解锁
				            lock.unLock();
				        }
					}catch(Exception e){
						
					}				
				}
			});
    		thread.start();
    	}
	}

}
