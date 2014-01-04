package org.nutz.ssdb.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import org.nutz.ssdb.spi.Cmd;
import org.nutz.ssdb.spi.Respose;
import org.nutz.ssdb.spi.SSDBException;
import org.nutz.ssdb.spi.SSDBStream;

public abstract class AbstractIoSSDBStream implements SSDBStream {

	protected InputStream in;

	protected OutputStream out;

	public synchronized Respose req(Cmd cmd, byte[]... vals) {
		beforeExec();
		try {
			write(cmd.bytes());
			for (byte[] bs : vals) {
				write(bs);
			}
			out.write('\n');
			out.flush();
			Respose resp = new Respose();
			resp.stat = new String(read());
			resp.datas = new ArrayList<byte[]>();
			while (true) {
				int len = 0;
				int d = in.read();
				if (d == '\n')
					break;
				else if (d >= '0' && d <= '9')
					len = len * 10 + (d - '0');
				else
					throw new SSDBException("protocol error. unexpect byte=" + d);
				while (true) {
					d = in.read();
					if (d >= '0' && d <= '9')
						len = len * 10 + (d - '0');
					else if (d == '\n')
						break;
					else
						throw new SSDBException("protocol error. unexpect byte=" + d);
				}
				byte[] data = new byte[len];
				if (len > 0)
					in.read(data);
				d = in.read();
				if (d != '\n')
					throw new SSDBException("protocol error. unexpect byte=" + d);
				resp.datas.add(data);
			}
			beforeReturn(resp);
			return resp;
		} catch (Throwable e) {
			return whenError(e);
		}
	}

	protected byte[] read() throws IOException {
		int len = 0;
		int d = 0;
		while (true) {
			d = in.read();
			if (d >= '0' && d <= '9')
				len = len * 10 + (d - '0');
			else if (d == '\n')
				break;
			else
				throw new SSDBException("protocol error. unexpect byte=" + d);
		}
		byte[] data = new byte[len];
		if (len > 0)
			in.read(data);
		d = in.read();
		if (d != '\n')
			throw new SSDBException("protocol error. unexpect byte=" + d);
		return data;
	}

	protected void write(byte[] data) throws IOException {
		if (data == null)
			data = RawClient.EMPTY_ARG;
		out.write(Integer.toString(data.length).getBytes());
		out.write('\n');
		out.write(data);
		out.write('\n');
		out.flush();
	}

	protected void beforeExec() {
	}

	protected void beforeReturn(Respose resp) {
	}

	protected Respose whenError(Throwable e) {
		throw new SSDBException(e);
	}
}
