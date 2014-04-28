package org.nutz.ssdb4j.impl;

import java.util.List;

import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.ObjectConv;
import org.nutz.ssdb4j.spi.Respose;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;

public class SimpleClient implements SSDB {

	protected SSDBStream stream;
	
	protected ObjectConv conv;

	public SimpleClient(SSDBStream stream) {
		this.stream = stream;
		this.conv = DefaultObjectConv.me;
	}

	public SimpleClient(String host, int port, int timeout) {
		stream = new SocketSSDBStream(host, port, timeout);
		this.conv = DefaultObjectConv.me;
	}

	protected byte[] bytes(Object obj) {
		return conv.bytes(obj);
	}

	protected byte[][] bytess(Object... objs) {
		return conv.bytess(objs);
	}
	

	protected final Respose req(Cmd cmd, byte[] first, byte[][] lots) {
		byte[][] vals = new byte[lots.length+1][];
		vals[0] = first;
		for (int i = 0; i < lots.length; i++) {
			vals[i+1] = lots[i];
		}
		return req(cmd, vals);
	}
	
	protected Respose req(Cmd cmd, byte[] ... vals) {
		return stream.req(cmd, vals);
	}

	@Override
	public SSDB batch() {
		return new BatchClient(stream);
	}

	@Override
	public List<Respose> exec() {
		throw new SSDBException("not batch!");
	}
	
	public void setObjectConv(ObjectConv conv) {
		this.conv = conv;
	}
	
	public void setSSDBStream(SSDBStream stream) {
		this.stream = stream;
	}
	
	//----------------------------------------------------------------------------------

	public Respose get(Object key) {
		return req(Cmd.get,bytes(key));
	}

	@Override
	public Respose set(Object key, Object val) {
		return req(Cmd.set,bytes(key), bytes(val));
	}

	@Override
	public Respose setx(Object key, Object val, int ttl) {
		return req(Cmd.setx,bytes(key), bytes(val), (""+ttl).getBytes());
	}

	@Override
	public Respose del(Object key) {
		return req(Cmd.del,bytes(key));
	}

	@Override
	public Respose incr(Object key, int val) {
		return req(Cmd.incr,bytes(key), (""+val).getBytes());
	}

	@Override
	public Respose exists(Object key) {
		return req(Cmd.exists,bytes(key));
	}

	@Override
	public Respose keys(Object start, Object end, int limit) {
		return req(Cmd.keys,bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Respose multi_set(Object... pairs) {
		return req(Cmd.multi_set,bytess(pairs));
	}

	@Override
	public Respose multi_get(Object... keys) {
		return req(Cmd.multi_get,bytess(keys));
	}

	@Override
	public Respose multi_del(Object... keys) {
		return req(Cmd.multi_del,bytess(keys));
	}

	@Override
	public Respose scan(Object start, Object end, int limit) {
		return req(Cmd.scan,bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Respose rscan(Object start, Object end, int limit) {
		return req(Cmd.rscan,bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Respose hset(Object key, Object hkey, Object hval) {
		return req(Cmd.hset,bytes(key), bytes(hkey), bytes(hval));
	}

	@Override
	public Respose hdel(Object key, Object hkey) {
		return req(Cmd.hdel,bytes(key), bytes(hkey));
	}

	@Override
	public Respose hget(Object key, Object hkey) {
		return req(Cmd.hget,bytes(key), bytes(hkey));
	}

	@Override
	public Respose hsize(Object key) {
		return req(Cmd.hsize,bytes(key));
	}

	@Override
	public Respose hlist(Object key, Object hkey, int limit) {
		return req(Cmd.hlist,bytes(key), bytes(hkey), (""+limit).getBytes());
	}

	@Override
	public Respose hincr(Object key, Object hkey, int val) {
		return req(Cmd.hincr,bytes(key), bytes(hkey), (""+val).getBytes());
	}

	@Override
	public Respose hscan(Object key, Object start, Object end, int limit) {
		return req(Cmd.hscan,bytes(key), bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Respose hrscan(Object key, Object start, Object end, int limit) {
		return req(Cmd.hrscan,bytes(key), bytes(start), bytes(end), (""+limit).getBytes());
	}

	public Respose zset(Object key, Object zkey, int score) {
		return req(Cmd.zset,bytes(key), bytes(zkey), (""+score).getBytes());
	}

	@Override
	public Respose zget(Object key, Object zkey) {
		return req(Cmd.zget,bytes(key), bytes(zkey));
	}

	@Override
	public Respose zdel(Object key, Object zkey) {
		return req(Cmd.zdel,bytes(key), bytes(zkey));
	}

	@Override
	public Respose zincr(Object key, Object zkey, int val) {
		return req(Cmd.zincr,bytes(key), bytes(zkey), (""+val).getBytes());
	}

	@Override
	public Respose zsize(Object key) {
		return req(Cmd.zsize,bytes(key));
	}
	
	@Override
	public Respose zrank(Object key, Object zkey) {
		return req(Cmd.zrank,bytes(key), bytes(zkey));
	}

	@Override
	public Respose zrrank(Object key, Object zkey) {
		return req(Cmd.zrrank, bytes(key), bytes(zkey));
	}

	@Override
	public Respose zscan(Object key, Object zkey_start, int score_start, int score_end, int limit) {
		return req(Cmd.zscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), (""+limit).getBytes());
	}

	@Override
	public Respose zrscan(Object key, Object zkey_start, int score_start, int score_end, int limit) {
		return req(Cmd.zrscan, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), (""+limit).getBytes());
	}

	@Override
	public Respose qsize(Object key) {
		return req(Cmd.qsize, bytes(key));
	}

	@Override
	public Respose qfront(Object key) {
		return req(Cmd.qfront, bytes(key));
	}

	@Override
	public Respose qback(Object key) {
		return req(Cmd.qback, bytes(key));
	}

	@Override
	public Respose qpush(Object key, Object value) {
		return req(Cmd.qpush, bytes(key), bytes(value));
	}

	@Override
	public Respose qpop(Object key) {
		return req(Cmd.qpop, bytes(key));
	}


	@Override
	public Respose hkeys(Object key, Object start, Object end, int limit) {
		return req(Cmd.hkeys, bytes(key), bytes(start), bytes(end), (""+limit).getBytes());
	}

	@Override
	public Respose hexists(Object key, Object hkey) {
		return req(Cmd.hexists, bytes(key), bytes(hkey));
	}

	@Override
	public Respose hclear(Object key) {
		return req(Cmd.hclear, bytes(key));
	}

	@Override
	public Respose multi_hget(Object key, Object... hkeys) {
		return req(Cmd.multi_hget, bytes(key), bytess(hkeys));
	}

	@Override
	public Respose multi_hset(Object key, Object... pairs) {
		return req(Cmd.multi_hset, bytes(key), bytess(pairs));
	}

	@Override
	public Respose multi_hdel(Object key, Object... hkeys) {
		return req(Cmd.multi_hdel, bytes(key), bytess(hkeys));
	}

	@Override
	public Respose zexists(Object key, Object zkey) {
		return req(Cmd.zexists, bytes(key), bytes(zkey));
	}

	@Override
	public Respose zclear(Object key) {
		return req(Cmd.zclear, bytes(key));
	}

	@Override
	public Respose zkeys(Object key, Object zkey_start, int score_start, int score_end, int limit) {
		return req(Cmd.zkeys, bytes(key), bytes(zkey_start), bytes(score_start), bytes(score_end), (""+limit).getBytes());
	}

	@Override
	public Respose zrange(Object key, int offset, int limit) {
		return req(Cmd.zrange, bytes(key), (""+offset).getBytes(), (""+limit).getBytes());
	}

	@Override
	public Respose zrrange(Object key, int offset, int limit) {
		return req(Cmd.zrrange, bytes(key), (""+offset).getBytes(), (""+limit).getBytes());
	}

	@Override
	public Respose multi_zset(Object key, Object... pairs) {
		return req(Cmd.multi_zset, bytes(key), bytess(pairs));
	}

	@Override
	public Respose multi_zget(Object key, Object... zkeys) {
		return req(Cmd.multi_zget, bytes(key), bytess(zkeys));
	}

	@Override
	public Respose multi_zdel(Object key, Object... zkeys) {
		return req(Cmd.multi_zdel, bytes(key), bytess(zkeys));
	}

	@Override
	public Respose flushdb(Object key) {
		if (key != null)
			return req(Cmd.flushdb, bytes(key));
		return req(Cmd.flushdb);
	}

	@Override
	public Respose info() {
		return req(Cmd.info);
	}

	@Override
	public Respose ping() {
		return req(Cmd.ping);
	}
	
	//------------------------------------------

	@Override
	public Respose setnx(Object key, Object val) {
		return req(Cmd.setnx, bytes(key), bytes(val));
	}

	@Override
	public Respose getset(Object key, Object val) {
		return req(Cmd.getset, bytes(key), bytes(val));
	}

	@Override
	public Respose qslice(Object key, int start, int end) {
		return req(Cmd.qslice, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}

	@Override
	public Respose qget(Object key, int index) {
		return req(Cmd.qget, bytes(key), (""+index).getBytes());
	}

	@Override
	public Respose zcount(Object key, int start, int end) {
		return req(Cmd.zcount, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}

	@Override
	public Respose zsum(Object key, int start, int end) {
		return req(Cmd.zsum, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}

	@Override
	public Respose zavg(Object key, int start, int end) {
		return req(Cmd.zavg, bytes(key), (""+start).getBytes(), (""+end).getBytes());
	}
	
	
}
