package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.AVDBReplicatedServer;
import server.ReplicatedServer;

import org.apache.zookeeper.*;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class MyDBFaultTolerantServerZK extends AVDBReplicatedServer implements Watcher {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;

    private static final String ZK_ROOT = "/replicated_db";
    private static final String ZK_LEADER = ZK_ROOT + "/leader";
    private static final int SESSION_TIMEOUT = 5000;

    private ZooKeeper zk;
    private boolean isLeader = false;
    private String myID;

    private final ConcurrentHashMap<Long, JSONObject> requestQueue = new ConcurrentHashMap<>();
    private long nextRequestId = 0;
    private long expectedId = 0;

    private CopyOnWriteArrayList<String> notAcked;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
        super(nodeConfig, myID, isaDB); 
        this.myID = myID;

        try {
            zk = new ZooKeeper("localhost:2181", SESSION_TIMEOUT, this);
            if (zk.exists(ZK_ROOT, false) == null) {
                zk.create(ZK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            electLeader();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted && ZK_LEADER.equals(event.getPath())) {
            try {
                electLeader();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void electLeader() throws KeeperException, InterruptedException {
        try {
            zk.create(ZK_LEADER, myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            isLeader = true;
            System.out.println(myID + " is elected leader.");
        } catch (KeeperException.NodeExistsException e) {
            isLeader = false;
            zk.exists(ZK_LEADER, true); 
            System.out.println(myID + " is a follower.");
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String requestStr = new String(bytes);

        try {
            JSONObject json = new JSONObject();
            json.put("request", requestStr);
            json.put("type", "REQUEST");

            serverMessenger.send(getLeaderId(), json.toString().getBytes());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        try {
            JSONObject json = new JSONObject(new String(bytes));
            String type = json.getString("type");

            if ("REQUEST".equals(type)) {
                if (isLeader) {
                    long reqId = nextRequestId++;
                    json.put("reqId", reqId);
                    json.put("type", "PROPOSAL");

                    requestQueue.put(reqId, json);
                    broadcastProposal(reqId, json);
                }
            } else if ("PROPOSAL".equals(type)) {
                executeRequest(json);
                sendAck(header.sndr, json.getLong("reqId"));
            } else if ("ACK".equals(type) && isLeader) {
                processAck(json.getString("sender"), json.getLong("reqId"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeRequest(JSONObject json) throws Exception {
        String query = json.getString("request");
        session.execute(query);
    }

    private void sendAck(String leaderId, long reqId) throws IOException {
        JSONObject ack = new JSONObject();
        ack.put("type", "ACK");
        ack.put("reqId", reqId);
        ack.put("sender", myID);
        serverMessenger.send(leaderId, ack.toString().getBytes());
    }

    private void broadcastProposal(long reqId, JSONObject proposal) throws IOException {
        notAcked = new CopyOnWriteArrayList<>(serverMessenger.getNodeConfig().getNodeIDs());
        for (String node : serverMessenger.getNodeConfig().getNodeIDs()) {
            serverMessenger.send(node, proposal.toString().getBytes());
        }
    }

    private void processAck(String sender, long reqId) {
        notAcked.remove(sender);
        if (notAcked.isEmpty()) {
            requestQueue.remove(reqId);
        }
    }

    private String getLeaderId() throws KeeperException, InterruptedException {
        byte[] data = zk.getData(ZK_LEADER, false, null);
        return new String(data);
    }

    @Override
    public void close() {
        try { zk.close(); } catch (InterruptedException e) { e.printStackTrace(); }
        super.close();
    }

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.err.println("Usage: <server.properties> <myID> [DB address]");
                System.exit(1);
            }

            InetSocketAddress isaDB = args.length > 2
                    ? new InetSocketAddress(args[2].split(":")[0], Integer.parseInt(args[2].split(":")[1]))
                    : new InetSocketAddress("localhost", 9042);

            new MyDBFaultTolerantServerZK(
                    edu.umass.cs.nio.nioutils.NodeConfigUtils.getNodeConfigFromFile(
                            args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET),
                    args[1],
                    isaDB
            );

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
