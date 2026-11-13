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

            int expectedSeq = 0;
            int totalMessages = -1;
            StringBuilder reconstructed = new StringBuilder();

            // 2. Receive loop until all messages are received
            while (totalMessages == -1 || expectedSeq <= totalMessages) {
                byte[] buf = new byte[1024];
                DatagramPacket recvPacket = new DatagramPacket(buf, buf.length);
                socket.receive(recvPacket);

                // Simulate 50% packet loss if enabled
                if (dropHalf && random.nextDouble() < 0.5) {
                    System.out.println("[DROP] Simulated packet loss (no ACK sent).");
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
                        totalMessages = Integer.parseInt(payload.trim());
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
                byte[] ackBytes = ackMsg.getBytes();
                DatagramPacket ackPacket = new DatagramPacket(ackBytes, ackBytes.length, talkerIP, talkerPort);
                socket.send(ackPacket);
                System.out.println("[SENT] " + ackMsg);
            }

            // 3. Close connection and display concatenated string
            System.out.println("\nAll messages received. Closing connection.");
            System.out.println("Final Reconstructed String: " + reconstructed.toString());

        } catch (Exception e) {
            System.err.println("Listener Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}