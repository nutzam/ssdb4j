package org.nutz.ssdb4j.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.nutz.ssdb4j.spi.Cmd;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.nutz.ssdb4j.spi.SSDBException;
import org.nutz.ssdb4j.spi.SSDBStream;

public class BatchClient extends SimpleClient {

    protected static Response OK = new Response();
    static {
        OK.stat = "ok";
    }

    protected List<_Req> reqs;

    protected List<Response> resps = new ArrayList<Response>();
    
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
        for (_Req req : reqs) {
            this.resps.add(stream.req(req.cmd, req.vals));
        }
        List<Response> resps = this.resps;
        this.resps = null;
        this.reqs = null;
        return resps;
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
