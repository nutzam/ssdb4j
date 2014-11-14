package org.nutz.ssdb4j.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDBException;

public class SocketSSDBStream extends AbstractIoSSDBStream {

	private Socket socket;
	protected String host;
	protected int port;
	protected int timeout;
	protected byte[] auth;

	public SocketSSDBStream(String host, int port, int timeout) {
		this(host, port, timeout, null);
	}
	
	public SocketSSDBStream(String host, int port, int timeout, byte[] auth) {
        this.socket = new Socket();
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.auth = auth;
    }

	protected void beforeExec() {
		if (!socket.isConnected()) {
			try {
				socket.connect(new InetSocketAddress(host, port), timeout);
				socket.setSoTimeout(timeout);
				this.in = new BufferedInputStream(socket.getInputStream());
				this.out = new BufferedOutputStream(socket.getOutputStream());
				if (auth != null) {
				    SSDBs.sendCmd(out, Cmd.auth, auth);
		            if (!SSDBs.readResp(in).ok()) {
		                throw new IOException("auth fail");
		            }
				}
			} catch (IOException e) {
				throw new SSDBException(e);
			}
		}
	}

	@Override
	protected Response whenError(Throwable e) {
		if (!socket.isClosed())
			try {
				socket.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		return super.whenError(e);
	}
	
	public void close() throws IOException {
		socket.close();
	}
}
