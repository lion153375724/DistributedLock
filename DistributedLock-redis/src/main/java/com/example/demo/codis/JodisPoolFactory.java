package com.example.demo.codis;

import io.codis.jodis.JedisResourcePool;
import io.codis.jodis.RoundRobinJedisPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;

import com.example.demo.conf.Conf;

public class JodisPoolFactory {
	
	private Logger logger = LoggerFactory.getLogger(JodisPoolFactory.class);
	private static JedisResourcePool jedisPool;
	private static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
	
	private static volatile JodisTemplate jodisTemplate;
	
	public static JodisTemplate getJodisTemplate(){
		if(null == jodisTemplate){
			synchronized (JodisPoolFactory.class) {
				if(null == jodisTemplate){
					jodisTemplate = createJodisTemplate();
				}
			}
		}
		return jodisTemplate;
	}

	public static JodisTemplate createJodisTemplate(){
		initJedisPoolConfig();
		initRoundRobinJedisPool();
		return new JodisTemplate(jedisPool);
	}
	
    public static JedisPoolConfig initJedisPoolConfig(){
        jedisPoolConfig.setMaxTotal(Conf.getInt("jedis.maxTotal"));
        jedisPoolConfig.setMaxIdle(Conf.getInt("jedis.maxIdle"));
        jedisPoolConfig.setMinIdle(Conf.getInt("jedis.minIdle"));
        jedisPoolConfig.setNumTestsPerEvictionRun(Conf.getInt("jedis.numTestsPerEvictionRun"));
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(Conf.getLong("jedis.timeBetweenEvictionRunsMillis"));
        jedisPoolConfig.setMinEvictableIdleTimeMillis(Conf.getLong("jedis.minEvictableIdleTimeMillis"));
        jedisPoolConfig.setSoftMinEvictableIdleTimeMillis(Conf.getLong("jedis.softMinEvictableIdleTimeMillis"));
        jedisPoolConfig.setMaxWaitMillis(Conf.getLong("jedis.maxWaitMillis"));
        jedisPoolConfig.setTestOnBorrow(Conf.getBoolean("jedis.testOnBorrow"));
        jedisPoolConfig.setTestWhileIdle(Conf.getBoolean("jedis.testWhileIdle"));
        jedisPoolConfig.setTestOnReturn(Conf.getBoolean("jedis.maxTotal"));
        jedisPoolConfig.setJmxEnabled(Conf.getBoolean("jedis.testOnReturn"));
        jedisPoolConfig.setBlockWhenExhausted(Conf.getBoolean("jedis.blockWhenExhausted"));
        jedisPoolConfig.setTestOnCreate(Conf.getBoolean("jedis.testOnCreate"));
        return jedisPoolConfig;
    }

	public static JedisResourcePool initRoundRobinJedisPool() {
    	System.out.println("enter....");
    	jedisPool = RoundRobinJedisPool.create().poolConfig(jedisPoolConfig).curatorClient(Conf.getString("redis.zkAddr"), 30000).zkProxyDir("/zk/codis/db_test/proxy").build();
    	//jedisPool = new JedisPool(jedisPoolConfig,env.getProperty("redis.zkAddr"),30000);
    	
		/*roundRobinJedisPool = RoundRobinJedisPool.create().poolConfig(jedisPoolConfig).timeoutMs(Integer.parseInt(env.getProperty("redis.timeoutMs")))
				.curatorClient(env.getProperty("redis.zkAddr"), Integer.parseInt(env.getProperty("redis.zkSessionTimeoutMs")))
				.zkProxyDir("/zk/codis/db_test/proxy").build();*/
		return jedisPool;
    }
	
}
