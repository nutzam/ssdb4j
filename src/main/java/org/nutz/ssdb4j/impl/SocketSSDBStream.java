package org.nutz.ssdb4j.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.nutz.ssdb4j.spi.Respose;
import org.nutz.ssdb4j.spi.SSDBStream;

public class SocketSSDBStream extends AbstractIoSSDBStream {

	private Socket socket;
	protected String host;
	protected int port;
	protected int timeout;
	
	public SocketSSDBStream(String host, int port, int timeout) {
		this.socket = new Socket();
		this.host = host;
		this.port = port;
		this.timeout = timeout;
	}
	
	protected void beforeExec() {
		if (!socket.isConnected()) {
			try {
				socket.connect(new InetSocketAddress(host, port), timeout);
				this.in = socket.getInputStream();
				this.out = socket.getOutputStream();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	@Override
	protected Respose whenError(Throwable e) {
		if (socket.isConnected())
			try {
				socket.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		return super.whenError(e);
	}
	
	public SSDBStream doClone() {
		return new SocketSSDBStream(host, port, timeout);
	}
	
	protected void finalize() throws Throwable {
		if (socket != null)
			socket.close();
	}
}
