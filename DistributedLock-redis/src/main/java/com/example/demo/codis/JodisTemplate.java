package com.example.demo.codis;

import io.codis.jodis.JedisResourcePool;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

/** jodis抽象模板类

 *
 */
public class JodisTemplate implements RedisTemplateInterface{

	private Logger logger = LoggerFactory.getLogger(JodisPoolFactory.class);
	
	/**连接池。
	 * 
	 */
	private JedisResourcePool jedisResourcePool;
	
	public JodisTemplate(JedisResourcePool jedisResourcePool) {
		this.jedisResourcePool = jedisResourcePool;
	}

	public Jedis getJedis() {
        for (int i = 1; i < 4; i++) {
          try {
            Jedis jedis = jedisResourcePool.getResource();
            if (jedis.isConnected()) {
              return jedis;
            }
          } catch (Exception e) {
            logger.error("get jedis "+i+" time failer*****************************************");
          }
        }
		return null;
	}
	
	@Override
	public String set(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.set(key, value);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public String set(String key, String value, String nxxx, String expx, long time) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.set(key, value, nxxx, expx, time);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public String get(String key) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.get(key);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public Long exists(String... keys) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.exists(keys);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public Boolean exists(String key) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.exists(key);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public Long del(String... keys) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.del(keys);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public Long del(String key) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.del(key);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public String type(String key) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.type(key);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public Set<String> keys(String pattern) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.keys(pattern);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}



	@Override
	public Long expire(String key, int seconds) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.expire(key, seconds);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	


	@Override
	public String setex(String key, int seconds, String value) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.setex(key, seconds, value);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	

	@Override
	public Long hset(String key, String field, String value) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.hset(key, field, value);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public String hget(String key, String field) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.hget(key, field);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}
	
	@Override
	public String hmset(String key, Map<String, String> hash) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.hmset(key, hash);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.hmget(key, fields);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public Long hincrBy(String key, String field, long value) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.hincrBy(key, field, value);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public String set(byte[] key, byte[] value) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.set(key, value);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	@Override
	public byte[] get(byte[] key) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
			return jedis.get(key);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}
	
	 /**
     * setnx
     * 
     * @param key
     * @param value
     * @param seconds
     * @return
     */
    public boolean setnx(String key, String value, int seconds) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
            Long setnx = jedis.setnx(key.getBytes(), value.getBytes());
            if (setnx == 1) {
            	System.out.println("seconds:"+seconds);
                return jedis.expire(key, seconds) == 1;
            }
            return false;
        } catch (Exception e) {
        	jedis.close();
            return false;
        }
    }

    /**
     * getSet
     * 
     * @param key
     * @return
     */
    public String getSet(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = this.getJedis();
            return jedis.getSet(key, value);
        } catch (Exception e) {
        	jedis.close();
            return null;
        }
    }

}
