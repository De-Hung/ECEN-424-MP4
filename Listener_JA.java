import java.net.*;
import java.util.*;

public class Listener {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java Listener <talkerIP> <talkerPort> <listenerPort> [dropHalfPackets: true|false]");
            return;
        }

        String talkerIPStr = args[0];
        int talkerPort;
        int listenerPort;
        boolean dropHalf = false;

        try {
            talkerPort = Integer.parseInt(args[1]);
            listenerPort = Integer.parseInt(args[2]);
            if (args.length >= 4) {
                dropHalf = Boolean.parseBoolean(args[3]);
            }
        } catch (NumberFormatException e) {
            System.out.println("Error: Ports must be valid integers.");
            return;
        }

        Random random = new Random();

        try (DatagramSocket socket = new DatagramSocket(listenerPort)) {
            InetAddress talkerIP = InetAddress.getByName(talkerIPStr);

            // 1. Send UDP connection request to Talker and start listening
            String connectReq = "CONNECT";
            byte[] reqBytes = connectReq.getBytes();
            DatagramPacket reqPacket = new DatagramPacket(reqBytes, reqBytes.length, talkerIP, talkerPort);
            socket.send(reqPacket);
            System.out.println("Sent connection request to Talker (" + talkerIPStr + ":" + talkerPort + ")");
            System.out.println("Listener listening on port " + listenerPort + " (50% Drop Mode: " + dropHalf + ")");

            socket.setSoTimeout(3000);       // lets us retry CONNECT / give up if the Talker vanishes
            int idleTimeouts = 0;
            final int MAX_IDLE = 10;         // 10 x 3 s = 30 s of silence -> give up

            int expectedSeq = 0;
            int totalMessages = -1;
            StringBuilder reconstructed = new StringBuilder();

            // 2. Receive loop until all messages are received
            while (totalMessages == -1 || expectedSeq <= totalMessages) {
                byte[] buf = new byte[1024];
                DatagramPacket recvPacket = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(recvPacket);
                    idleTimeouts = 0;
                } catch (SocketTimeoutException e) {
                    idleTimeouts++;
                    if (idleTimeouts >= MAX_IDLE) {
                        System.out.println("[ERROR] No packets from Talker for " + (MAX_IDLE * 3) + " s. Giving up.");
                        return;
                    }
                    if (totalMessages == -1) {
                        // Talker may not have been running yet when the first CONNECT was sent
                        socket.send(reqPacket);
                        System.out.println("[INFO] No reply yet. Re-sending connection request...");
                    }
                    continue;
                }

                String rawMsg = new String(recvPacket.getData(), 0, recvPacket.getLength());
                int colonIdx = rawMsg.indexOf(':');
                if (colonIdx == -1) {
                    System.out.println("[WARN] Malformed packet received: " + rawMsg);
                    continue;
                }

                int seqNum;
                try {
                    seqNum = Integer.parseInt(rawMsg.substring(0, colonIdx));
                } catch (NumberFormatException e) {
                    System.out.println("[WARN] Invalid sequence number in packet: " + rawMsg);
                    continue;
                }

                String payload = rawMsg.substring(colonIdx + 1);
                System.out.println("[RECV] Message ID: " + seqNum + " | Payload: \"" + payload + "\"");

                if (seqNum == expectedSeq) {
                    if (seqNum == 0) {
                        try {
                            totalMessages = Integer.parseInt(payload.trim());
                        } catch (NumberFormatException e) {
                            System.out.println("[WARN] Invalid message count in message 0: " + payload);
                            continue;
                        }
                        System.out.println("[INFO] Expecting " + totalMessages + " data messages.");
                    } else {
                        reconstructed.append(payload);
                    }
                    expectedSeq++;
                } else {
                    System.out.println("[DUPLICATE] Expected ID " + expectedSeq + ", got " + seqNum + ". Re-sending ACK.");
                }

                // Send ACK containing sequence number of NEXT expected message
                String ackMsg = "ACK:" + expectedSeq;

                // Simulate the Listener failing to ACK ~50% of packets (the data HAS been received/stored)
                if (dropHalf && random.nextDouble() < 0.5) {
                    System.out.println("[DROP] Withholding " + ackMsg + " for Message ID " + seqNum + " (no ACK sent).");
                    continue;
                }
                byte[] ackBytes = ackMsg.getBytes();
                DatagramPacket ackPacket = new DatagramPacket(ackBytes, ackBytes.length, talkerIP, talkerPort);
                socket.send(ackPacket);
                System.out.println("[SENT] " + ackMsg);
            }

            // 3. All messages received: display the concatenated string
            System.out.println("\nAll messages received.");
            System.out.println("Final Reconstructed String: " + reconstructed.toString());

            // 4. Linger before closing. If our final ACK was lost, the Talker will retransmit
            //    the last message; keep re-ACKing until the line has been quiet for 5 s.
            socket.setSoTimeout(5000);
            try {
                while (true) {
                    byte[] lbuf = new byte[1024];
                    DatagramPacket lingerPacket = new DatagramPacket(lbuf, lbuf.length);
                    socket.receive(lingerPacket);
                    System.out.println("[DUPLICATE] Late retransmit after completion. Re-sending ACK.");
                    String lastAck = "ACK:" + expectedSeq;
                    if (dropHalf && random.nextDouble() < 0.5) {
                        System.out.println("[DROP] Withholding " + lastAck + " (no ACK sent).");
                        continue;
                    }
                    byte[] lastAckBytes = lastAck.getBytes();
                    socket.send(new DatagramPacket(lastAckBytes, lastAckBytes.length, talkerIP, talkerPort));
                    System.out.println("[SENT] " + lastAck);
                }
            } catch (SocketTimeoutException e) {
                // quiet for 5 s: Talker has everything, safe to close
            }
            System.out.println("Connection closed.");

        } catch (Exception e) {
            System.err.println("Listener Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}