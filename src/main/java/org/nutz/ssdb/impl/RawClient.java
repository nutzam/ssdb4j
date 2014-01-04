package org.nutz.ssdb.impl;

import org.nutz.ssdb.spi.Cmd;
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
		return stream.req(Cmd.GET, key);
	}

	
	public Respose set(byte[] key, byte[] val) {
		return stream.req(Cmd.SET, key, val);
	}
	
	public Respose setx(byte[] key, byte[] val, int ttl) {
		return stream.req(Cmd.SETX, key, val, (""+ttl).getBytes());
	}

	
	public Respose del(byte[] key) {
		return stream.req(Cmd.DEL, key);
	}

	
	public Respose incr(byte[] key, int val) {
		return stream.req(Cmd.INCR, key, (""+val).getBytes());
	}
	
	@Override
	public Respose exists(byte[] key) {
		return stream.req(Cmd.EXISTS, key);
	}
	
	@Override
	public Respose keys(byte[] start, byte[] end, int limit) {
		return stream.req(Cmd.KEYS, start, end, (""+limit).getBytes());
	}

	
	public Respose multi_set(byte[] ... pairs) {
		return stream.req(Cmd.MULTI_SET, pairs);
	}

	@Override
	public Respose multi_get(byte[]... keys) {
		return stream.req(Cmd.MULTI_GET, keys);
	}
	
	public Respose multi_del(byte[] ... keys) {
		return stream.req(Cmd.MULTI_DEL, keys);
	}

	
	public Respose scan(byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req(Cmd.SCAN, start, end, (""+limit).getBytes());
	}

	
	public Respose rscan(byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req(Cmd.RSCAN, start, end, (""+limit).getBytes());
	}

	
	public Respose hset(byte[] key, byte[] hkey, byte[] hval) {
		return stream.req(Cmd.HSET, key, hkey, hval);
	}

	
	public Respose hdel(byte[] key, byte[] hkey) {
		return stream.req(Cmd.HDEL, key, hkey);
	}

	
	public Respose hget(byte[] key, byte[] hkey) {
		return stream.req(Cmd.HGET, key, hkey);
	}

	
	public Respose hsize(byte[] key) {
		return stream.req(Cmd.HSIZE, key);
	}

	
	public Respose hlist(byte[] key, byte[] hkey, int limit) {
		return stream.req(Cmd.HLIST, key, hkey,(""+limit).getBytes());
	}

	
	public Respose hincr(byte[] key, byte[] hkey, int val) {
		return stream.req(Cmd.HINCR, key, hkey, (""+val).getBytes());
	}

	
	public Respose hscan(byte[] key, byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req(Cmd.HSCAN, key, start, end, (""+limit).getBytes());
	}

	
	public Respose hrscan(byte[] key, byte[] start, byte[] end, int limit) {
		if (start == null)
			start = EMPTY_ARG;
		if (end == null)
			end = EMPTY_ARG;
		return stream.req(Cmd.HRSCAN, key, start, end, (""+limit).getBytes());
	}

	
	public Respose zset(byte[] key, byte[] zkey, double score) {
		return stream.req(Cmd.ZSET, key, zkey, new Double(score).toString().getBytes());
	}

	
	public Respose zget(byte[] key, byte[] zkey) {
		return stream.req(Cmd.ZGET, key, zkey);
	}

	
	public Respose zdel(byte[] key, byte[] zkey) {
		return stream.req(Cmd.ZDEL, key, zkey);
	}

	
	public Respose zincr(byte[] key, byte[] zkey, int val) {
		return stream.req(Cmd.ZINCR, key, zkey, (""+val).getBytes());
	}

	
	public Respose zsize(byte[] key) {
		return stream.req(Cmd.ZSIZE, key);
	}

	
	public Respose zrank(byte[] key, byte[] zkey) {
		return stream.req(Cmd.ZRANK, key, zkey);
	}

	
	public Respose zrrank(byte[] key, byte[] zkey) {
		return stream.req(Cmd.ZRRANK, key, zkey);
	}

	
	public Respose zrange(byte[] key, byte[] zkey, int limit) {
		return stream.req(Cmd.ZRANGE, key, zkey, (""+limit).getBytes());
	}

	
	public Respose zrrange(byte[] key, byte[] zkey, int limit) {
		return stream.req(Cmd.ZRRANGE, key, zkey, (""+limit).getBytes());
	}

	
	public Respose zscan(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req(Cmd.ZSCAN, key, start, end, (""+limit).getBytes());
	}

	
	public Respose zrscan(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req(Cmd.ZRSCAN, key, start, end, (""+limit).getBytes());
	}

	
	public Respose qsize(byte[] key) {
		return stream.req(Cmd.QSIZE, key);
	}

	
	public Respose qfront(byte[] key) {
		return stream.req(Cmd.QFRONT, key);
	}

	
	public Respose qback(byte[] key) {
		return stream.req(Cmd.QBACK, key);
	}

	
	public Respose qpush(byte[] key, byte[] value) {
		return stream.req(Cmd.QPUSH, key, value);
	}

	
	public Respose qpop(byte[] key) {
		return stream.req(Cmd.QPOP, key);
	}


	@Override
	public Respose hkeys(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req(Cmd.HKEYS, start, end, (""+limit).getBytes());
	}


	@Override
	public Respose hexists(byte[] key, byte[] hkey) {
		return stream.req(Cmd.HEXISTS, key, hkey);
	}


	@Override
	public Respose hclear(byte[] key) {
		return stream.req(Cmd.HCLEAR, key);
	}


	@Override
	public Respose multi_hget(byte[] key, byte[]... hkeys) {
		byte[][] data = new byte[hkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = hkeys[i-1];
		}
		return stream.req(Cmd.MULTI_HGET, data);
	}


	@Override
	public Respose multi_hset(byte[] key, byte[]... pairs) {
		byte[][] data = new byte[pairs.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = pairs[i-1];
		}
		return stream.req(Cmd.MULTI_HSET, data);
	}


	@Override
	public Respose multi_hdel(byte[] key, byte[]... hkeys) {
		byte[][] data = new byte[hkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = hkeys[i-1];
		}
		return stream.req(Cmd.MULTI_HDEL, data);
	}


	@Override
	public Respose zexists(byte[] key, byte[] zkey) {
		return stream.req(Cmd.ZEXISTS, key, zkey);
	}


	@Override
	public Respose zclear(byte[] key) {
		return stream.req(Cmd.ZCLEAR, key);
	}


	@Override
	public Respose zkeys(byte[] key, byte[] start, byte[] end, int limit) {
		return stream.req(Cmd.ZKEYS, start, end, (""+limit).getBytes());
	}


	@Override
	public Respose zrange(byte[] key, int offset, int limit) {
		return stream.req(Cmd.ZRANGE, (""+offset).getBytes(), (""+limit).getBytes());
	}


	@Override
	public Respose zrrange(byte[] key, int offset, int limit) {
		return stream.req(Cmd.ZRRANGE, (""+offset).getBytes(), (""+limit).getBytes());
	}


	@Override
	public Respose multi_zset(byte[] key, byte[]... pairs) {
		byte[][] data = new byte[pairs.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = pairs[i-1];
		}
		return stream.req(Cmd.MULTI_ZSET, data);
	}


	@Override
	public Respose multi_zget(byte[] key, byte[]... zkeys) {
		byte[][] data = new byte[zkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = zkeys[i-1];
		}
		return stream.req(Cmd.MULTI_ZGET, data);
	}


	@Override
	public Respose multi_zdel(byte[] key, byte[]... zkeys) {
		byte[][] data = new byte[zkeys.length + 1][];
		data[0] = key;
		for (int i = 1; i < data.length; i++) {
			data[i] = zkeys[i-1];
		}
		return stream.req(Cmd.MULTI_ZDEL, data);
	}

	@Override
	public Respose flushdb(byte[] key) {
		if (key == null)
			return stream.req(Cmd.FLUSHDB);
		return stream.req(Cmd.FLUSHDB, key);
	}
	
	@Override
	public Respose info() {
		return stream.req(Cmd.INFO);
	}
	
	@Override
	public Respose ping() {
		return stream.req(Cmd.PING);
	}
}
