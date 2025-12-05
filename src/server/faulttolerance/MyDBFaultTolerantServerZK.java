package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;


public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;

    protected final MessageNIOTransport<String, String> serverMessenger;
    protected final String myID;
    protected final String leader;

    private final Cluster cluster;
    private final Session session;

    private final ConcurrentHashMap<Long, JSONObject> queue = new ConcurrentHashMap<>();
    private CopyOnWriteArrayList<String> notAcked;
    private long reqNum = 0;
    private long expected = 0;

    private synchronized long incrReqNum() { return reqNum++; }
    private synchronized long incrExpected() { return expected++; }

    public static enum Type {
        REQUEST, PROPOSAL, ACKNOWLEDGEMENT
    }

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID,
                                     InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB, myID);

        this.myID = myID;

        this.cluster = Cluster.builder().addContactPoint(isaDB.getHostString()).build();
        this.session = cluster.connect(myID);

        this.serverMessenger = new MessageNIOTransport<>(myID, nodeConfig,
                new AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        handleMessageFromServer(bytes, nioHeader);
                        return true;
                    }
                }, true);

        String firstNode = null;
        for (String node : nodeConfig.getNodeIDs()) {
            firstNode = node;
            break;
        }
        this.leader = firstNode;

        log.log(Level.INFO, "Server {0} started with leader {1}", new Object[]{myID, leader});
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String requestStr = new String(bytes);
        log.log(Level.INFO, "{0} received client request: {1}", new Object[]{myID, requestStr});

        JSONObject packet = new JSONObject();
        try {
            packet.put("request", requestStr);
            packet.put("type", Type.REQUEST.toString());
            // Forward to leader
            serverMessenger.send(leader, packet.toString().getBytes());
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        JSONObject json;
        try {
            json = new JSONObject(new String(bytes));
        } catch (JSONException e) {
            e.printStackTrace();
            return;
        }

        String type;
        try {
            type = json.getString("type");
        } catch (JSONException e) {
            log.severe("Malformed message received: " + json);
            return;
        }

        try {
            if (type.equals(Type.REQUEST.toString())) {
                if (myID.equals(leader)) {
                    long reqId = incrReqNum();
                    json.put("reqId", reqId);
                    queue.put(reqId, json);
                    sendNextProposalIfReady();
                }
            } else if (type.equals(Type.PROPOSAL.toString())) {
                String query = json.getString("request");
                long reqId = json.getLong("reqId");

                session.execute(query);

                String senderId = serverMessenger.getNodeConfig().getNodeID(header.sndr);
                JSONObject ack = new JSONObject();
                ack.put("type", Type.ACKNOWLEDGEMENT.toString());
                ack.put("reqId", reqId);
                ack.put("node", myID);
                serverMessenger.send(senderId, ack.toString().getBytes());
            } else if (type.equals(Type.ACKNOWLEDGEMENT.toString())) {
                if (myID.equals(leader)) {
                    String node = json.getString("node");
                    if (dequeue(node)) {
                        expected++;
                        sendNextProposalIfReady();
                    }
                }
            }
        } catch (JSONException | IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isReadyToSend(long expectedId) {
        return queue.containsKey(expectedId);
    }

    private void sendNextProposalIfReady() throws IOException, JSONException {
        if (isReadyToSend(expected)) {
            JSONObject proposal = queue.remove(expected);
            if (proposal != null) {
                proposal.put("type", Type.PROPOSAL.toString());
                enqueue();
                broadcastProposal(proposal);
            }
        }
    }

    private void broadcastProposal(JSONObject proposal) throws IOException {
        for (String node : serverMessenger.getNodeConfig().getNodeIDs()) {
            serverMessenger.send(node, proposal.toString().getBytes());
        }
        log.log(Level.INFO, "Leader {0} broadcast proposal {1}", new Object[]{myID, proposal});
    }

    private void enqueue() {
        notAcked = new CopyOnWriteArrayList<>();
        notAcked.addAll(serverMessenger.getNodeConfig().getNodeIDs());
    }

    private boolean dequeue(String node) {
        notAcked.remove(node);
        return notAcked.isEmpty();
    }

    @Override
    public void close() {
        super.close();
        serverMessenger.stop();
        session.close();
        cluster.close();
    }

    public static void main(String[] args) throws IOException {
        NodeConfig<String> config = NodeConfigUtils.getNodeConfigFromFile(args[0],
                ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
        String myID = args[1];
        InetSocketAddress isaDB = args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                : new InetSocketAddress("localhost", 9042);
        new MyDBFaultTolerantServerZK(config, myID, isaDB);
    }
}
