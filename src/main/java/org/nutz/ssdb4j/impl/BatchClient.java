package org.nutz.ssdb4j.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;
import org.nutz.ssdb4j.spi.SSDBStreamCallback;

public class BatchClient extends SimpleClient implements SSDBStreamCallback {

    protected static Response OK = new Response();
    static {
        OK.stat = "ok";
    }

    protected List<_Req> reqs;

    protected Object respLock = new Object();

    protected int count;

    protected List<Response> resps;
    
    protected int timeout;
    
    protected TimeUnit timeUnit;

    public BatchClient(SSDBStream stream, int timeout, TimeUnit timeUnit) {
        super(stream);
        if (timeout < 0 || timeUnit == null)
            throw new IllegalArgumentException("timeout must bigger than 0, and timeUnit must not null");
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.resps = new ArrayList<Response>();
        this.reqs = new ArrayList<BatchClient._Req>();
    }

    public Response req(Cmd cmd, byte[]... vals) {
        if (reqs == null)
            throw new SSDBException("this BatchClient is invaild!");
        reqs.add(new _Req(cmd, vals));
        return OK;
    }

    public synchronized List<Response> exec() {
        if (reqs == null)
            throw new SSDBException("this BatchClient is invaild!");
        count = reqs.size();
        stream.callback(this);
        List<Response> resps = this.resps;
        this.resps = null;
        return resps;
    }

    public void invoke(final InputStream in, final OutputStream out) {
        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(new Callable<Object>() {
            public Object call() throws Exception {
                for (int i = 0; i < count; i++) {
                    resps.add(SSDBs.readResp(in));
                }
                return null;
            }
        });
        es.submit(new Callable<Object>() {
            public Object call() throws Exception {
                for (_Req req : reqs) {
                    SSDBs.writeBlock(out, req.cmd.bytes());
                    for (byte[] bs : req.vals) {
                        SSDBs.writeBlock(out, bs);
                    }
                    out.write('\n');
                }
                out.flush();
                return null;
            }
        });
        try {
            es.shutdown();
            boolean flag = es.awaitTermination(timeout, timeUnit);
            reqs = null;
            if (!flag)
                throw new RuntimeException(new TimeoutException("batch execute timeout!"));
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            es.shutdownNow();
        }
    }

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
