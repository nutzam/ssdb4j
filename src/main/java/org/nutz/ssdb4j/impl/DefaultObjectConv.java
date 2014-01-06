package org.nutz.ssdb4j.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Byteable;
import org.nutz.ssdb4j.spi.ObjectConv;
import org.nutz.ssdb4j.spi.SSDBException;

public class DefaultObjectConv implements ObjectConv {
	
	public static ObjectConv me = new DefaultObjectConv();
	public static byte[] NULL = "null".getBytes();
	public static byte[][] NULLs = new byte[][]{};
	
	protected Charset charset = SSDBs.DEFAULT_CHARSET;

	public byte[] bytes(Object obj) {
		if (obj == null)
			return NULL;
		if (obj instanceof byte[])
			return (byte[]) obj;
		if (obj instanceof Byteable)
			return ((Byteable)obj).bytes();
		if (obj instanceof InputStream) {
			InputStream in = (InputStream)obj;
			byte[] data;
			try {
				data = new byte[in.available()];
				in.read(data);
			} catch (IOException e) {
				throw new SSDBException(e);
			} finally {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
			return data;
		}
		return obj.toString().getBytes(charset);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public byte[][] bytess(Object... objs) {
		if (objs == null)
			return NULLs;
		if (objs instanceof byte[][])
			return (byte[][]) objs;
		if (objs.length == 1) {
			Object arg = objs[0];
			if (arg instanceof Map) {
				Map<Object, Object> map = (Map<Object, Object>) arg;
				byte[][] args = new byte[map.size() * 2][];
				int i = 0;
				for (Entry<Object, Object> en : map.entrySet()) {
					args[i] = bytes(en.getKey());
					args[i + 1] = bytes(en.getValue());
					i += 2;
				}
				return args;
			}
			if (arg instanceof Collection) {
				arg = ((Collection)arg).iterator();
			}
			if (arg instanceof Iterator) {
				List<byte[]> list = new ArrayList<byte[]>();
				Iterator it = (Iterator)arg;
				while (it.hasNext()) {
					list.add(bytes(it.next()));
				}
				return list.toArray(new byte[list.size()][]);
			}
			return new byte[][]{bytes(arg)};
		}
		byte[][] args = new byte[objs.length][];
		for (int i = 0; i < args.length; i++) {
			args[i] = bytes(objs[i]);
		}
		return args;
	}
}
