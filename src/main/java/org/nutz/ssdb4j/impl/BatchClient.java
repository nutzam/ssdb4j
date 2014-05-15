package org.nutz.ssdb4j.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public class BatchClient extends SimpleClient implements SSDBStreamCallback, Runnable {

	protected static Response OK = new Response();
	static {
		OK.stat = "ok";
	}
	
	protected List<_Req> reqs;
	
	protected Object respLock = new Object();
	
	protected InputStream _in;
	
	protected int count;
	
	protected List<Response> resps;
	
	public BatchClient(SSDBStream stream) {
		super(stream);
		this.resps = new ArrayList<Response>();
		this.reqs = new ArrayList<BatchClient._Req>();
	}

	@Override
	public Response req(Cmd cmd, byte[]... vals) {
		if (reqs == null)
			throw new SSDBException("this BatchClient is invaild!");
		reqs.add(new _Req(cmd, vals));
		return OK;
	}
	
	@Override
	public synchronized List<Response> exec() {
		if (reqs == null)
			throw new SSDBException("this BatchClient is invaild!");
		count = reqs.size();
		stream.callback(this);
		List<Response> resps = this.resps;
		this.resps = null;
		return resps;
	}
	
	@Override
	public void invoke(InputStream in, OutputStream out) {
		_in = in;
		new Thread(this).start();
		try {
			for (_Req req : reqs) {
				SSDBs.writeBlock(out, req.cmd.bytes());
				for (byte[] bs : req.vals) {
					SSDBs.writeBlock(out, bs);
				}
				out.write('\n');
			}
			out.flush();
			reqs = null;
			synchronized (respLock) {
				respLock.wait();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new SSDBException(e);
		}
	}
	
	@Override
	public void run() {
		try {
			for (int i = 0; i < count; i++) {
				resps.add(SSDBs.readResp(_in));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			_in = null;
			synchronized (respLock) {
				respLock.notifyAll();
			}
		}
		
	}
	
	@Override
	public SSDB batch() {
		throw new SSDBException("aready in batch mode, not support for batch again");
	}
	
	static class _Req {
		public Cmd cmd;
		public byte[][] vals;
		public _Req(Cmd cmd, byte[]... vals) {
			this.cmd = cmd;
			this.vals = vals;
		}
	}
}
