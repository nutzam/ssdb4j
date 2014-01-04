package org.nutz.ssdb.spi;

/**
 * SSDB for java 的标准接口
 * @author wendal(wendal1985@gmail.com)
 * <p/> ssdb官方暂未提供具体方法的时间复杂度数据.
 */
public interface SSDB {

	/**根据key获取一个值*/
	Respose get(Object key);
	/**设置一个key-val对*/
	Respose set(Object key, Object val);
	/**设置一个key-val对,并设置过期时间,单位为秒*/
	Respose setx(Object key, Object val, int ttl);
	/**根据key删除一个val*/
	Respose del(Object key);
	/**自增*/
	Respose incr(Object key, int val);
	/**是否存在*/
	Respose exists(Object key);
	/**遍历键*/
	Respose keys(Object start, Object end, int limit);
	/**批量set*/
	Respose multi_set(Object ... pairs);
	Respose multi_get(Object ... keys);
	/**批量删除*/
	Respose multi_del(Object ... keys);
	
	//-----
	Respose scan(Object start, Object end, int limit);
	Respose rscan(Object start, Object end, int limit);
	//----
	
	Respose hset(Object key, Object hkey, Object hval);
	Respose hdel(Object key, Object hkey);
	Respose hget(Object key, Object hkey);
	Respose hsize(Object key);
	Respose hlist(Object key, Object hkey, int limit);
	Respose hincr(Object key, Object hkey, int val);
	//-----
	Respose hscan(Object key, Object start, Object end, int limit);
	Respose hrscan(Object key, Object start, Object end, int limit);
	Respose hkeys(Object key, Object start, Object end, int limit);
	Respose hexists(Object key, Object hkey);
	Respose hclear(Object key);
	Respose multi_hget(Object key, Object ...hkeys);
	Respose multi_hset(Object key, Object ...pairs);
	Respose multi_hdel(Object key, Object ...hkeys);
	//-----
	
	Respose zset(Object key, Object zkey, double score);
	Respose zget(Object key, Object zkey);
	Respose zdel(Object key, Object zkey);
	Respose zincr(Object key, Object zkey, int val);
	Respose zsize(Object key);
	Respose zrank(Object key, Object zkey);
	Respose zrrank(Object key, Object zkey);
	Respose zexists(Object key, Object zkey);
	Respose zclear(Object key);
	
	Respose zkeys(Object key, Object start, Object end,int limit);
	Respose zscan(Object key, Object start, Object end,int limit);
	Respose zrscan(Object key, Object start, Object end,int limit);
	
	Respose zrange(Object key, int offset, int limit);
	Respose zrrange(Object key, int offset, int limit);
	
	Respose multi_zset(Object key, Object ... pairs);
	Respose multi_zget(Object key, Object ... zkeys);
	Respose multi_zdel(Object key, Object ... zkeys);
	
	//-----------
	Respose qsize(Object key);
	Respose qfront(Object key);
	Respose qback(Object key);
	Respose qpush(Object key, Object value);
	Respose qpop(Object key);
	
	//---------------
	
	/**还没实现啊,啊啊啊啊啊*/
	SSDB batch();
	
	/**还没实现啊,啊啊啊啊啊*/
	Respose exec();
}
