import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class DracoRequestProcessor extends Thread {

    //private Socket socket = null;
    private LinkedBlockingQueue<Socket> sockets = new LinkedBlockingQueue<Socket>();
    int socket_num = 2;
    int READ_REQUEST = 1;
    int WRITE_REQUEST = 2;
    int WRITE_OK = 1000;
    PrintStream out = null;
    InputStreamReader in = null;
    DataOutputStream dout = null;
    DataInputStream din = null;
    boolean debug = false;
    boolean lat_test = false;
    String threadName;

    protected LinkedBlockingQueue<Request> queuedRequests =
            new LinkedBlockingQueue<Request>();

    //RequestProcessor nextProcessor;

    protected volatile boolean stopped = false;
    private long workerShutdownTimeoutMS;
    //protected WorkerService workerPool;

    public DracoRequestProcessor(String id, int socket_num) {
        //super("DracoRequestProcessor:" + id);
        int port = 5500;
	this.socket_num = socket_num;
        //this.nextProcessor = nextProcessor;
        try {
            for (int i = 0; i < socket_num; i++) {
                Socket socket = new Socket(InetAddress.getLocalHost(), port);
                socket.setTcpNoDelay(true);
                sockets.add(socket);
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (!stopped) {
            Request rq;
            Socket socket;
            try {
                rq = this.queuedRequests.take();
		if (rq.type == OpCode.closeSession) {
			stopped = true;
			break;
		}
                socket = sockets.take();
                sendToNextProcessor(rq, socket);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }

    @Override
    public void start() {
        //int numCores = Runtime.getRuntime().availableProcessors();
        //int numWorkerThreads = 12;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request, Socket socket) {
        //workerPool.schedule(new DracoWorkRequest(request, socket), request.sessionId);
	//System.out.println("Queue: " + queuedRequests.size() + " " +  sockets.size());
        DracoWorkRequest dwr = new DracoWorkRequest(request, socket);
        dwr.start();
    }

    private class DracoWorkRequest implements Runnable {
        private Thread t;
        private final Request rq;
        private final Socket socket;
        DracoWorkRequest(Request request, Socket socket) {
            this.rq = request;
            this.socket = socket;
        }

        public String get(String key) throws IOException {
            int bufSize = 8 + key.length();
            ByteBuffer buffer = ByteBuffer.allocate(bufSize);
            buffer.putInt(READ_REQUEST);
            buffer.putInt(key.length());
            buffer.put(key.getBytes());
            byte[] data = buffer.array();
            socket.getOutputStream().write(data);
            byte[] valLenBt = new byte[4];
            int rc = socket.getInputStream().read(valLenBt);
            ByteBuffer valLenBuf = ByteBuffer.wrap(valLenBt);
            int valLen = valLenBuf.getInt();
            byte[] valueBt = new byte[valLen];
            rc = socket.getInputStream().read(valueBt);
            String value = new String (valueBt);
            return value;
        }

        public void put(String key, String value) throws IOException {
            long start = System.nanoTime();
            //System.out.println("PUT: " + key + " " + value);
            int bufSize = 12 + key.length() + value.length();
            ByteBuffer buffer = ByteBuffer.allocate(bufSize);
            buffer.putInt(WRITE_REQUEST);
            buffer.putInt(key.length());
            buffer.put(key.getBytes());
            buffer.putInt(value.length());
            buffer.put(value.getBytes());
            byte[] data = buffer.array();
            socket.getOutputStream().write(data);

            byte[] ackBt = new byte[4];
            int rc = socket.getInputStream().read(ackBt);
            ByteBuffer ackBuf = ByteBuffer.wrap(ackBt);
            int ack = ackBuf.getInt();
            //System.out.println("PUT ACK: " + ack);
            if (lat_test) {
                long end = System.nanoTime();
                long microseconds = (end-start)/1000;
            }
        }

        public void cleanup() {
            if (!stopped) {
                DracoRequestProcessor.this.halt();
            }
        }

        public void run() {
            try {

                if (rq.type == OpCode.create) {
                    put(rq.path, rq.value);
                    rq.dracoDone = true;
                } else if (rq.type == OpCode.getData) {
                    rq.dracoReturnVal = get(rq.path);
                    rq.dracoDone = true;
                    System.out.println("GET: " + rq.dracoReturnVal);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            DracoRequestProcessor.this.sockets.add(socket);
            //nextProcessor.processRequest(rq);
        }

        // Start Sender thread
        public void start() {
            if (t == null) {
                t = new Thread (this, this.rq.path);
                t.start();
            }
        }
    }

    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        queuedRequests.add(request);
    }

    private void halt() {
        stopped = true;
        queuedRequests.clear();
    }

    public void shutdown() {
        halt();
    }

}
