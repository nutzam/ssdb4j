package org.nutz.ssdb4j.spi;

import java.io.Closeable;
import java.util.List;

/**
 * SSDB for java 的标准接口
 * @author wendal(wendal1985@gmail.com)
 */
public interface SSDB extends Closeable {

	/**根据key获取一个值*/
	Response get(Object key);
	/**设置一个key-val对*/
	Response set(Object key, Object val);
	/**设置一个key-val对,并设置过期时间,单位为秒*/
	Response setx(Object key, Object val, int ttl);
	/**根据key删除一个val*/
	Response del(Object key);
	/**自增*/
	Response incr(Object key, int val);
	Response decr(Object key, int val);
	/**是否存在*/
	Response exists(Object key);
	Response multi_exists(Object...keys);
	/**遍历键*/
	Response keys(Object start, Object end, int limit);
	/**批量set*/
	Response multi_set(Object ... pairs);
	Response multi_get(Object ... keys);
	/**批量删除*/
	Response multi_del(Object ... keys);
	
	//-----
	Response scan(Object start, Object end, int limit);
	Response rscan(Object start, Object end, int limit);
	//----
	
	Response hset(Object key, Object hkey, Object hval);
	Response hdel(Object key, Object hkey);
	Response hget(Object key, Object hkey);
	Response hsize(Object key);
	Response hlist(Object key, Object hkey, int limit);
	Response hincr(Object key, Object hkey, int val);
	Response hdecr(Object key, Object hkey, int val);
	//-----
	Response hscan(Object key, Object start, Object end, int limit);
	Response hrscan(Object key, Object start, Object end, int limit);
	Response hkeys(Object key, Object start, Object end, int limit);
	Response hexists(Object key, Object hkey);
	Response hclear(Object key);
	Response hgetall(Object key);
	Response hvals(Object key, Object start, Object end, int limit);
	Response multi_hget(Object key, Object ...hkeys);
	Response multi_hset(Object key, Object ...pairs);
	Response multi_hdel(Object key, Object ...hkeys);
	Response multi_hexists(Object ...keys);
	Response multi_hsize(Object ...keys);
	//-----
	/*2014.05.15之前的官方驱动中参数类型是double,那是错误的*/
	Response zset(Object key, Object zkey, long score);
	Response zget(Object key, Object zkey);
	Response zdel(Object key, Object zkey);
	Response zincr(Object key, Object zkey, int val);
	Response zdecr(Object key, Object zkey, int val);
	Response zlist(Object key_start, Object key_end, int limit);
	Response zsize(Object key);
	Response zrank(Object key, Object zkey);
	Response zrrank(Object key, Object zkey);
	Response zexists(Object key, Object zkey);
	Response zclear(Object key);
	
	Response zremrangebyrank(Object key, Object score_start, Object score_end);
	Response zremrangebyscore(Object key, Object score_start, Object score_end);
	
	Response zkeys(Object key, Object zkey_start, Object score_start, Object score_end, int limit);
	Response zscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit);
	Response zrscan(Object key, Object zkey_start, Object score_start, Object score_end, int limit);
	
	Response zrange(Object key, int offset, int limit);
	Response zrrange(Object key, int offset, int limit);
	
	Response multi_zset(Object key, Object ... pairs);
	Response multi_zget(Object key, Object ... zkeys);
	Response multi_zdel(Object key, Object ... zkeys);
	Response multi_zexists(Object key, Object ...zkeys);
	Response multi_zsize(Object ...keys);
	
	//-----------
	Response qsize(Object key);
	Response qfront(Object key);
	Response qback(Object key);
	Response qpush(Object key, Object value);
	Response qpush_front(Object key, Object value);
	Response qpush_back(Object key, Object value);
	Response qpop(Object key);
	Response qpop_front(Object key);
	Response qpop_back(Object key);
	Response qfix(Object key);
	Response qlist(Object key_start, Object key_end, int limit);
	Response qclear(Object key);
	Response qrange(Object key, int begin, int limit);
	
	Response flushdb(String type);
	Response info();
	Response ping();
	
	//---------------
	
	/**
	 * 批量执行,注意: 返回值是新的SSDB实例!!
	 * <p></p>由于当前版本的服务器不支持pipe,所以均为客户端缓存,务必留意内存问题
	 * 
	 */
	SSDB batch(); // TODO 支持命令合并机制
	
	/**
	 * 将缓存中的命令发送到服务器并依次获取返回值
	 * @return 一系列响应
	 */
	List<Response> exec();
	

	/*=================================================================*/
	/*==================add at 1.6.8.5=================================*/
	/*=================================================================*/

	/**如果key不存在,就执行set操作*/
	Response setnx(Object key, Object val);
	/**取值并更新值*/
	Response getset(Object key, Object val);
	
	Response qslice(Object key, int start, int end);
	
	Response qget(Object key, int index);
	
	/*=================================================================*/
	/*==================add at 1.6.8.6=================================*/
	/*=================================================================*/
	
	Response zcount(Object key, int start, int end);
	Response zsum(Object key, int start, int end);
	Response zavg(Object key, int start, int end);
	
	@Deprecated/**官方ssdb尚不支持*/
	Response eval(Object lua, Object... args);
	@Deprecated/**官方ssdb尚不支持*/
	Response evalsha(Object sha1, Object... args);
	
	/*=================================================================*/
	/*==================add at 1.6.8.7=================================*/
	/*=================================================================*/
	
	Response ttl(Object key);
	Response expire(Object key, int ttl);
	
	//------------------------------------------------------------------
	Response key_range();
	Response compact();
	
	/*=================================================================*/
    /*==================add at 1.6.8.8=================================*/
    /*=================================================================*/
    Response getbit(Object key, int offset);
    Response setbit(Object key, int offset, byte on);
    Response countbit(Object key, int start, int size);
    Response substr(Object key, int start, int size);
    Response getrange(Object key, int start, int size);
    Response strlen(Object key);
    Response redis_bitcount(Object key, int start, int size);
    Response hrlist(Object key, Object hkey, int limit);
    Response zrlist(Object zkey_start, Object zkey_end, int limit);
    Response qrlist(Object key_start, Object key_end, int limit);
    
    /*=================================================================*/
    /*==================add at 1.7.0=================================*/
    /*=================================================================*/
    
    Response auth(String passwd);
    Response qtrim_front(Object key, int size);
    Response qtrim_back(Object key, int size);
	
	
	/*=================================================================*/
	/*==================一些管理方法,非ssdb指令=========================*/
	/*=================================================================*/

	void changeObjectConv(ObjectConv conv);
	
	Response req(Cmd cmd, byte[] ... values);
	
	/*=================================================================*/
	/*==================内部命令?======================================*/
	/*=================================================================*/
	@Deprecated Response dump();
	@Deprecated Response sync140();
	@Deprecated Response clear_binlog();
}
