package org.nutz.ssdb4j.spi;

public interface RawSSDB {

	Respose get(byte[] key);
	Respose set(byte[] key, byte[] val);
	Respose setx(byte[] key, byte[] val, int ttl);
	Respose del(byte[] key);
	Respose incr(byte[] key, int val);
	Respose exists(byte[] key);
	Respose keys(byte[] start, byte[] end, int limit);
	Respose multi_set(byte[] ... pairs);
	Respose multi_get(byte[] ... keys);
	Respose multi_del(byte[] ... keys);
	
	//-----
	Respose scan(byte[] start, byte[] end, int limit);
	Respose rscan(byte[] start, byte[] end, int limit);
	//----
	
	Respose hset(byte[] key, byte[] hkey, byte[] hval);
	Respose hdel(byte[] key, byte[] hkey);
	Respose hget(byte[] key, byte[] hkey);
	Respose hsize(byte[] key);
	Respose hlist(byte[] key, byte[] hkey, int limit);
	Respose hincr(byte[] key, byte[] hkey, int val);
	//-----
	Respose hscan(byte[] key, byte[] start, byte[] end, int limit);
	Respose hrscan(byte[] key, byte[] start, byte[] end, int limit);
	Respose hkeys(byte[] key, byte[] start, byte[] end, int limit);
	Respose hexists(byte[] key, byte[] hkey);
	Respose hclear(byte[] key);
	Respose multi_hget(byte[] key, byte[] ...hkeys);
	Respose multi_hset(byte[] key, byte[] ...pairs);
	Respose multi_hdel(byte[] key, byte[] ...hkeys);
	//-----
	
	Respose zset(byte[] key, byte[] zkey, double score);
	Respose zget(byte[] key, byte[] zkey);
	Respose zdel(byte[] key, byte[] zkey);
	Respose zincr(byte[] key, byte[] zkey, int val);
	Respose zsize(byte[] key);
	Respose zrank(byte[] key, byte[] zkey);
	Respose zrrank(byte[] key, byte[] zkey);
	Respose zexists(byte[] key, byte[] zkey);
	Respose zclear(byte[] key);
	
	Respose zkeys(byte[] key, byte[] start, byte[] end,int limit);
	Respose zscan(byte[] key, byte[] start, byte[] end,int limit);
	Respose zrscan(byte[] key, byte[] start, byte[] end,int limit);
	
	Respose zrange(byte[] key, int offset, int limit);
	Respose zrrange(byte[] key, int offset, int limit);
	
	Respose multi_zset(byte[] key, byte[] ... pairs);
	Respose multi_zget(byte[] key, byte[] ... zkeys);
	Respose multi_zdel(byte[] key, byte[] ... zkeys);
	
	//-----------
	Respose qsize(byte[] key);
	Respose qfront(byte[] key);
	Respose qback(byte[] key);
	Respose qpush(byte[] key, byte[] value);
	Respose qpop(byte[] key);
	
	Respose flushdb(byte[] key);
	Respose info();
	Respose ping();
}
