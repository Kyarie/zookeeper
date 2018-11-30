public class Main {

    public static void main(String[] args) {
        DracoRequestProcessor drp = new DracoRequestProcessor("i");
        for (int i = 0; i < 5000; i++) {
            Request rq = new Request(OpCode.create, "key" + i, "value" + i);
            drp.processRequest(rq);
        }
    }
}
