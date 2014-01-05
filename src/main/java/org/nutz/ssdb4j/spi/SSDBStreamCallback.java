package org.nutz.ssdb4j.spi;

import java.io.InputStream;
import java.io.OutputStream;

public interface SSDBStreamCallback {

	void invoke(InputStream in, OutputStream out);
}
