/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.messaging;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jsr166y.LinkedTransferQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;

public class ForeignHost {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private Connection m_connection;
    private Connection m_connection2;
    final FHInputHandler m_handler;
    final FHInputHandler m_handler2;
    private final HostMessenger m_hostMessenger;
    final int m_hostId;
    final InetSocketAddress m_listeningAddress;
    final LinkedTransferQueue<DeferredSerialization> outQueue = new LinkedTransferQueue<DeferredSerialization>();
    private final Thread m_sendThread = new Thread() {

        @Override
        public void run() {
            try {
                m_writeSC.configureBlocking(true);
                m_writeSocket.setTcpNoDelay(false);
                while (true) {
                    DeferredSerialization ds = outQueue.take();
                    while (ds != null) {
                        for (ByteBuffer b : ds.serialize()) {
                           while (b.hasRemaining()) {
                               m_writeSC.write(b);
                           }
                        }
                        ds = outQueue.poll(500, TimeUnit.MICROSECONDS);
                    }
                }
//                BufferedOutputStream bos = new BufferedOutputStream(m_socket.getOutputStream());
//                while (true) {
//                    DeferredSerialization ds = outQueue.take();
//
//                    while (ds != null) {
//                        for (ByteBuffer b : ds.serialize()) {
//                            System.out.println("Sending message " + b.remaining());
//                            bos.write(b.array(), b.arrayOffset() + b.position(), b.remaining());
//                        }
//                        ds = outQueue.poll();
//                    }
//                    bos.flush();
//                    System.out.println("Finished flush");
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    };

    private final Thread m_readThread = new Thread() {
        @Override
        public void run() {
            try {
                m_readSC.configureBlocking(true);
                m_readSocket.setTcpNoDelay(false);
                ByteBuffer lengthPrefixBuffer = ByteBuffer.allocateDirect(4);
                boolean hadMessage = false;
                while (true) {
                    lengthPrefixBuffer.clear();
                    final long startSpin = System.nanoTime();
                    while (lengthPrefixBuffer.hasRemaining()) {
                        final int read = m_readSC.read(lengthPrefixBuffer);
                        if (read == 0) {
                            final long now = System.nanoTime();
                            if (now - startSpin > 500000) {
                                hadMessage = false;
                                m_readSC.configureBlocking(true);
                            }
                        }
                    }
                    lengthPrefixBuffer.flip();

                    int nextLength = lengthPrefixBuffer.getInt();
                    ByteBuffer message = ByteBuffer.allocate(nextLength);
                    while (message.hasRemaining()) {
                        m_readSC.read(message);
                    }
                    message.flip();
                    handleRead( message, null);

                    if (hadMessage) {
                        hadMessage = true;
                        m_readSC.configureBlocking(false);
                    }
                }
//                BufferedInputStream bis = new BufferedInputStream(m_socket.getInputStream());
//                ByteBuffer lengthPrefixBuffer = ByteBuffer.allocate(4);
//                while (true) {
//                    lengthPrefixBuffer.clear();
//
//                    int read = 0;
//                    while (read < 4) {
//                        int readThisTime = bis.read(lengthPrefixBuffer.array());
//                        if (readThisTime == -1) return;
//                        read += readThisTime;
//                    }
//
//                    int nextLength = lengthPrefixBuffer.getInt();
//                    System.out.println("Received message " + nextLength);
//                    ByteBuffer message = ByteBuffer.allocate(nextLength);
//                    read = 0;
//                    while (read < nextLength) {
//                        int readThisTime = bis.read(message.array(), read, nextLength - read);
//                        if (readThisTime == -1) return;
//                        read += readThisTime;
//                    }
//                    handleRead(message, null);
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    };

    private final String m_remoteHostname = "UNKNOWN_HOSTNAME";
    private boolean m_closing;
    boolean m_isUp;

    // hold onto the socket so we can kill it
    private final Socket m_writeSocket;
    private final SocketChannel m_writeSC;
    private final Socket m_readSocket;
    private final SocketChannel m_readSC;

    // Set the default here for TestMessaging, which currently has no VoltDB instance
    private final long m_deadHostTimeout;
    private long m_lastMessageMillis;

    /** ForeignHost's implementation of InputHandler */
    public class FHInputHandler extends VoltProtocolHandler {

        @Override
        public int getMaxRead() {
            return Integer.MAX_VALUE;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) throws IOException {
            handleRead(message, c);
        }

        @Override
        public void stopping(Connection c)
        {
            m_isUp = false;
            if (!m_closing)
            {
                m_hostMessenger.reportForeignHostFailed(m_hostId);
            }
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }
    }

    /** Create a ForeignHost and install in VoltNetwork */
    ForeignHost(HostMessenger host, int hostId, SocketChannel readSocket, SocketChannel writeSocket, int deadHostTimeout,
            InetSocketAddress listeningAddress)
    throws IOException
    {
        m_hostMessenger = host;
        m_handler = new FHInputHandler();
        m_handler2 = new FHInputHandler();
        m_hostId = hostId;
        m_closing = false;
        m_isUp = true;
        m_lastMessageMillis = Long.MAX_VALUE;
        m_readSC = readSocket;
        m_readSocket = readSocket.socket();
        m_writeSC = writeSocket;
        m_writeSocket = writeSocket.socket();
        m_deadHostTimeout = deadHostTimeout;
        m_listeningAddress = listeningAddress;
        hostLog.info("Heartbeat timeout to host: " + m_readSocket.getRemoteSocketAddress() + " is " +
                         m_deadHostTimeout + " milliseconds");
    }

    public void register(HostMessenger host) throws IOException {
        m_connection = host.getNetwork().registerChannel( m_readSC, m_handler, 0);
        m_connection2 = host.getNetwork().registerChannel( m_writeSC, m_handler2, 0);
    }

    public void enableRead() {
        m_connection.enableReadSelection();
        m_connection2.enableReadSelection();
//        m_readThread.start();
//        m_sendThread.start();
    }

    synchronized void close()
    {
        m_isUp = false;
        if (m_closing) return;
        m_closing = true;
        if (m_connection != null)
            m_connection.unregister();
    }

    /**
     * Used only for test code to kill this FH
     */
    void killSocket() {
        try {
            m_closing = true;
            m_readSocket.setKeepAlive(false);
            m_readSocket.setSoLinger(false, 0);
            m_writeSocket.setKeepAlive(false);
            m_writeSocket.setSoLinger(false, 0);
            Thread.sleep(25);
            m_readSocket.close();
            m_writeSocket.close();
            Thread.sleep(25);
            System.gc();
            Thread.sleep(25);
        }
        catch (Exception e) {
            // don't REALLY care if this fails
            e.printStackTrace();
        }
    }

    /*
     * Huh!? The constructor registers the ForeignHost with VoltNetwork so finalizer
     * will never get called unless the ForeignHost is unregistered with the VN.
     */
    @Override
    protected void finalize() throws Throwable
    {
        if (m_closing) return;
        close();
        super.finalize();
    }

    boolean isUp()
    {
        return m_isUp;
    }

    /** Send a message to the network. This public method is re-entrant. */
    void send(
            final List<Long> destinations,
            final VoltMessage message)
    {
        if (destinations.isEmpty()) {
            return;
        }
        if (destinations.size() == 1) {
        	Connection c;
	        if (CoreUtils.getSiteIdFromHSId(destinations.get(0)) % 2 == 0) {
	            c = m_connection;
	        } else {
	            c = m_connection2;
	        }
        	c.writeStream().enqueue(
		            new DeferredSerialization() {
		                @Override
		                public final ByteBuffer[] serialize() throws IOException{
		                    int len = 4            /* length prefix */
		                            + 8            /* source hsid */
		                            + 4            /* destinationCount */
		                            + 8  /* destination list */
		                            + message.getSerializedSize();
		                    ByteBuffer buf = ByteBuffer.allocate(len);
		                    buf.putInt(len - 4);
		                    buf.putLong(message.m_sourceHSId);
		                    buf.putInt(1);
		                    buf.putLong(destinations.get(0));
		                    try {
		            			message.flattenToBuffer(buf);
		            		} catch (IOException e) {
		            			// TODO Auto-generated catch block
		            			e.printStackTrace();
		            		}
		                    buf.flip();
		                    return new ByteBuffer[] { buf };
		                }

		                @Override
		                public final void cancel() {
		                    /*
		                     * Can this be removed?
		                     */
		                }
		            });
        } else {
            int len = 4            /* length prefix */
                    + 8            /* source hsid */
                    + 4            /* destinationCount */
                    + 8  /* destination list */
                    + message.getSerializedSize();
            ByteBuffer buf = ByteBuffer.allocate(len);
            buf.putInt(len - 4);
            buf.putLong(message.m_sourceHSId);
            buf.putInt(1);
            buf.putLong(destinations.get(0));
            try {
    			message.flattenToBuffer(buf);
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
            buf.flip();
	        for (final Long destination : destinations) {
	        	Connection c;
		        if (CoreUtils.getSiteIdFromHSId(destination) % 2 == 0) {
		            c = m_connection;
		        } else {
		            c = m_connection2;
		        }
		        final ByteBuffer copy = ByteBuffer.allocate(buf.remaining());
		        copy.put(buf.duplicate());
		        copy.putLong(16, destination);
		        copy.flip();
		        System.out.println("Sending message length " + copy.remaining());
		        c.writeStream().enqueue(
		            new DeferredSerialization() {
		                @Override
		                public final ByteBuffer[] serialize() throws IOException{
		                	return new ByteBuffer[] { copy };
		                }

		                @Override
		                public final void cancel() {
		                    /*
		                     * Can this be removed?
		                     */
		                }
		            });
	        }
        }

        long current_time = EstTime.currentTimeMillis();
        long current_delta = current_time - m_lastMessageMillis;
        // NodeFailureFault no longer immediately trips FHInputHandler to
        // set m_isUp to false, so use both that and m_closing to
        // avoid repeat reports of a single node failure
        if ((!m_closing && m_isUp) &&
            (current_delta > m_deadHostTimeout))
        {
            hostLog.error("DEAD HOST DETECTED, hostname: " + m_remoteHostname);
            hostLog.info("\tcurrent time: " + current_time);
            hostLog.info("\tlast message: " + m_lastMessageMillis);
            hostLog.info("\tdelta (millis): " + current_delta);
            hostLog.info("\ttimeout value (millis): " + m_deadHostTimeout);
            m_hostMessenger.reportForeignHostFailed(m_hostId);
        }
    }


    String hostname() {
        return m_remoteHostname;
    }

    /** Deliver a deserialized message from the network to a local mailbox */
    private void deliverMessage(long destinationHSId, VoltMessage message) {
        Mailbox mailbox = m_hostMessenger.getMailbox(destinationHSId);
        /*
         * At this point we are OK with messages going to sites that don't exist
         * because we are saying that things can come and go
         */
        if (mailbox == null) {
            System.err.printf("Message (%s) sent to unknown site id: %s @ (%s) at " +
                    m_hostMessenger.getHostId() + " from " + CoreUtils.hsIdToString(message.m_sourceHSId) + "\n",
                    message.getClass().getSimpleName(),
                    CoreUtils.hsIdToString(destinationHSId), m_readSocket.getRemoteSocketAddress().toString());
            /*
             * If it is for the wrong host, that definitely isn't cool
             */
            if (m_hostMessenger.getHostId() != (int)destinationHSId) {
                assert(false);
            }
            return;
        }
        // deliver the message to the mailbox
        mailbox.deliver(message);
    }

    /** Read data from the network. Runs in the context of Port when
     * data is available.
     * @throws IOException
     */
    private void handleRead(ByteBuffer in, Connection c) throws IOException {
        // port is locked by VoltNetwork when in valid use.
        // assert(m_port.m_lock.tryLock() == true);
        long recvDests[] = null;

        final long sourceHSId = in.getLong();
        final int destCount = in.getInt();
        if (destCount == -1) {//This is a poison pill
            byte messageBytes[] = new byte[in.getInt()];
            in.get(messageBytes);
            String message = new String(messageBytes, "UTF-8");
            message = String.format("Fatal error from id,hostname(%d,%s): %s",
                    m_hostId, hostname(), message);
            org.voltdb.VoltDB.crashLocalVoltDB(message, false, null);
        }

        recvDests = new long[destCount];
        for (int i = 0; i < destCount; i++) {
            recvDests[i] = in.getLong();
        }

        final VoltMessage message =
            m_hostMessenger.getMessageFactory().createMessageFromBuffer(in, sourceHSId);

        for (int i = 0; i < destCount; i++) {
            deliverMessage( recvDests[i], message);
        }

        //m_lastMessageMillis = System.currentTimeMillis();
        m_lastMessageMillis = EstTime.currentTimeMillis();

        // ENG-1608.  We sniff for FailureSiteUpdateMessages here so
        // that a node will participate in the failure resolution protocol
        // even if it hasn't directly witnessed a node fault.
        if (message instanceof FailureSiteUpdateMessage)
        {
            for (long failedHostId : ((FailureSiteUpdateMessage)message).m_failedHSIds) {
                m_hostMessenger.reportForeignHostFailed((int)failedHostId);
            }
        }
    }

    public void sendPoisonPill(String err) {
        byte errBytes[];
        try {
            errBytes = err.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return;
        }
        ByteBuffer message = ByteBuffer.allocate( 20 + errBytes.length);
        message.putInt(message.capacity() - 4);
        message.putLong(-1);
        message.putInt(-1);
        message.putInt(errBytes.length);
        message.put(errBytes);
        message.flip();
        m_connection.writeStream().enqueue(message);
    }
}
