package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import server.MyDBSingleServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import org.json.JSONObject;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;

    private final Cluster cluster;
    private final Session session;

    private final List<JSONObject> requestLog = new CopyOnWriteArrayList<>();
    private final Set<Long> executedRequests = ConcurrentHashMap.newKeySet();

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID,
                                     InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB, myID);

        cluster = Cluster.builder().addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort()).build();
        session = cluster.connect();

        recoverFromCheckpoint();
    }

    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            String msg = new String(bytes);
            JSONObject req = new JSONObject(msg);
            long reqId = req.getLong("reqId");

            if (!executedRequests.contains(reqId)) {
                session.execute(req.getString("query"));
                executedRequests.add(reqId);

                requestLog.add(req);
                if (requestLog.size() > MAX_LOG_SIZE) requestLog.remove(0);

                for (String node : nodeConfig.getNodeIDs()) {
                    if (!node.equals(getMyID())) {
                        sendToServer(node, msg.getBytes());
                    }
                }

                sendAck(header.sndr, reqId);

                if (executedRequests.size() % 50 == 0) checkpoint();
            } else {
                sendAck(header.sndr, reqId);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        try {
            String msg = new String(bytes);
            JSONObject req = new JSONObject(msg);
            long reqId = req.getLong("reqId");

            if (!executedRequests.contains(reqId)) {
                session.execute(req.getString("query"));
                executedRequests.add(reqId);

                requestLog.add(req);
                if (requestLog.size() > MAX_LOG_SIZE) requestLog.remove(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendAck(InetSocketAddress client, long reqId) {
        try {
            JSONObject ack = new JSONObject();
            ack.put("reqId", reqId);
            ack.put("status", "OK");
            send(client, ack.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkpoint() {
        try {
            StringBuilder sb = new StringBuilder();
            for (Long reqId : executedRequests) {
                sb.append(reqId).append(",");
            }
            String cql = "INSERT INTO checkpoint (server_id, executed) VALUES ('" + getMyID() + "', '" + sb.toString() + "');";
            session.execute(cql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void recoverFromCheckpoint() {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS checkpoint (server_id text PRIMARY KEY, executed text);");
            ResultSet rs = session.execute("SELECT executed FROM checkpoint WHERE server_id='" + getMyID() + "';");
            if (!rs.isExhausted()) {
                String executedStr = rs.one().getString("executed");
                if (executedStr != null && !executedStr.isEmpty()) {
                    for (String id : executedStr.split(",")) {
                        executedRequests.add(Long.parseLong(id));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
        super.close();
    }

    public static void main(String[] args) throws IOException {
        NodeConfig<String> nodeConfig = NodeConfigUtils.getNodeConfigFromFile(
                args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
        String myID = args[1];
        InetSocketAddress isaDB = args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                : new InetSocketAddress("localhost", 9042);
        new MyDBFaultTolerantServerZK(nodeConfig, myID, isaDB);
    }
}
