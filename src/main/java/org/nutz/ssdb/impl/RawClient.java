package org.nutz.ssdb.impl;

import org.nutz.ssdb.spi.RawSSDB;
import org.nutz.ssdb.spi.Respose;
import org.nutz.ssdb.spi.SSDBStream;


public class RawClient implements RawSSDB {
	
	protected SSDBStream stream;
	
	public RawClient(SSDBStream stream) {
		this.stream = stream;
	}

	public static final byte[] EMPTY_ARG = new byte[0];
	
	public Respose get(byte[] key) {
		return stream.req("get", key);
	}

	
	public Respose set(byte[] key, byte[] val) {
		return stream.req("set", key, val);
	}
	
	public Respose setx(byte[] key, byte[] val, int ttl) {
		return stream.req("setx", key, val, (""+ttl).getBytes());
	}

	
	public Respose del(byte[] key) {
		return stream.req("del", key);
	}

	
	public Respose incr(byte[] key, int val) {
		return stream.req("incr", key, (""+val).getBytes());
	}
	
	@Override
	public Respose exists(byte[] key) {
		return stream.req("exists", key);
	}
	
	@Override
	public Respose keys(byte[] start, byte[] end, int limit) {
		return stream.req("keys", start, end, (""+limit).getBytes());
	}

	
	public Respose multi_set(byte[] ... pairs) {
		return stream.req("multi_set", pairs);
	}

	@Override
	public Respose multi_get(byte[]... keys) {
		return stream.req("multi_get", keys);
	}
	
	public Respose multi_del(byte[] ... keys) {
		return stream.req("multi_del", keys);
	}

	
	public Respose scan(byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req("scan", start, end, (""+limit).getBytes());
	}

	
	public Respose rscan(byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req("rscan", start, end, (""+limit).getBytes());
	}

	
	public Respose hset(byte[] key, byte[] hkey, byte[] hval) {
		return stream.req("hset", key, hkey, hval);
	}

	
	public Respose hdel(byte[] key, byte[] hkey) {
		return stream.req("hdel", key, hkey);
	}

	
	public Respose hget(byte[] key, byte[] hkey) {
		return stream.req("hget", key, hkey);
	}

	
	public Respose hsize(byte[] key) {
		return stream.req("hsize", key);
	}

	
	public Respose hlist(byte[] key, byte[] hkey, int limit) {
		return stream.req("hget", key, hkey,(""+limit).getBytes());
	}

	
	public Respose hincr(byte[] key, byte[] hkey, int val) {
		return stream.req("hincr", key, hkey, (""+val).getBytes());
	}

	
	public Respose hscan(byte[] key, byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req("hscan", key, start, end, (""+limit).getBytes());
	}

	
	public Respose hrscan(byte[] key, byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req("hrscan", key, start, end, (""+limit).getBytes());
	}

	
	public Respose zset(byte[] key, byte[] zkey, double score) {
		return stream.req("zset", key, zkey, new Double(score).toString().getBytes());
	}

	
	public Respose zget(byte[] key, byte[] zkey) {
		return stream.req("zget", key, zkey);
	}

	
	public Respose zdel(byte[] key, byte[] zkey) {
		return stream.req("zdel", key, zkey);
	}

	
	public Respose zincr(byte[] key, byte[] zkey, int val) {
		return stream.req("zincr", key, zkey, (""+val).getBytes());
	}

	
	public Respose zsize(byte[] key) {
		return stream.req("zsize", key);
	}

	//---------------------------------------------------------
	// 剩下的还不知道如何实现,晚点吧
	//---------------------------------------------------------
	
	public Respose zrank(byte[] key, byte[] zkey) {
		return stream.req("zrank", key, zkey);
	}

	
	public Respose zrrank(byte[] key, byte[] zkey) {
		return stream.req("zrrank", key, zkey);
	}

	
	public Respose zrange(byte[] key, byte[] zkey, int limit) {
		return stream.req("zrange", key, zkey, (""+limit).getBytes());
	}

	
	public Respose zrrange(byte[] key, byte[] zkey, int limit) {
		return stream.req("zrrange", key, zkey, (""+limit).getBytes());
	}

	
	public Respose zscan(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req("zscan", key, start, end, (""+limit).getBytes());
	}

	
	public Respose zrscan(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req("zrscan", key, start, end, (""+limit).getBytes());
	}

	
	public Respose qsize(byte[] key) {
		return stream.req("qsize", key);
	}

	
	public Respose qfront(byte[] key) {
		return stream.req("qfront", key);
	}

	
	public Respose qback(byte[] key) {
		return stream.req("qback", key);
	}

	
	public Respose qpush(byte[] key, byte[] value) {
		return stream.req("qpush", key, value);
	}

	
	public Respose qpop(byte[] key) {
		return stream.req("qpop", key);
	}


	@Override
	public Respose hkeys(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req("hkeys", start, end, (""+limit).getBytes());
	}


	@Override
	public Respose hexists(byte[] key, byte[] hkey) {
		return stream.req("hexists", key, hkey);
	}


	@Override
	public Respose hclear(byte[] key) {
		return stream.req("hclear", key);
	}


	@Override
	public Respose multi_hget(byte[] key, byte[]... hkeys) {
		byte[][] data = new byte[hkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = hkeys[i-1];
		}
		return stream.req("multi_hget", data);
	}


	@Override
	public Respose multi_hset(byte[] key, byte[]... pairs) {
		byte[][] data = new byte[pairs.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = pairs[i-1];
		}
		return stream.req("multi_hset", data);
	}


	@Override
	public Respose multi_hdel(byte[] key, byte[]... hkeys) {
		byte[][] data = new byte[hkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = hkeys[i-1];
		}
		return stream.req("multi_hdel", data);
	}


	@Override
	public Respose zexists(byte[] key, byte[] zkey) {
		return stream.req("zexists", key, zkey);
	}


	@Override
	public Respose zclear(byte[] key) {
		return stream.req("zclear", key);
	}


	@Override
	public Respose zkeys(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req("zkeys", start, end, (""+limit).getBytes());
	}


	@Override
	public Respose zrange(byte[] key, int offset, int limit) {
		return stream.req("zrange", (""+offset).getBytes(), (""+limit).getBytes());
	}


	@Override
	public Respose zrrange(byte[] key, int offset, int limit) {
		return stream.req("zrrange", (""+offset).getBytes(), (""+limit).getBytes());
	}


	@Override
	public Respose multi_zset(byte[] key, byte[]... pairs) {
		byte[][] data = new byte[pairs.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = pairs[i-1];
		}
		return stream.req("multi_zset", data);
	}


	@Override
	public Respose multi_zget(byte[] key, byte[]... zkeys) {
		byte[][] data = new byte[zkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = zkeys[i-1];
		}
		return stream.req("multi_zget", data);
	}


	@Override
	public Respose multi_zdel(byte[] key, byte[]... zkeys) {
		byte[][] data = new byte[zkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = zkeys[i-1];
		}
		return stream.req("multi_zdel", data);
	}

}
