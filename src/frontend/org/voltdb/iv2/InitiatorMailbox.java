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

package org.voltdb.iv2;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import jsr166y.LinkedTransferQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.messaging.RejoinMessage;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox
{
    static boolean LOG_TX = false;
    static boolean LOG_RX = false;

    VoltLogger hostLog = new VoltLogger("HOST");
    VoltLogger tmLog = new VoltLogger("TM");

    private final int m_partitionId;
    private final Scheduler m_scheduler;
    private final HostMessenger m_messenger;
    private final RepairLog m_repairLog;
    private final RejoinProducer m_rejoinProducer;
    private final LeaderCacheReader m_masterLeaderCache;
    private long m_hsId;
    private RepairAlgo m_algo;

    // hacky temp txnid
    long m_txnId = 0;


    private final ExecutorService m_es = Executors.newSingleThreadExecutor();
    final LinkedTransferQueue<Runnable> m_taskQueue = new LinkedTransferQueue<Runnable>();

    public void setRepairAlgo(final RepairAlgo algo)
    {
        FutureTask<Object> ft = new FutureTask<Object>(new Runnable() {
            @Override
            public void run() {
                m_algo = algo;
            }
        }, null);
        m_taskQueue.offer(ft);
        try {
            ft.get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void setLeaderState(final long maxSeenTxnId)
    {
        FutureTask<Object> ft = new FutureTask<Object>(new Runnable() {
            @Override
            public void run() {
                m_repairLog.setLeaderState(true);
                m_scheduler.setMaxSeenTxnId(maxSeenTxnId);
                m_scheduler.setLeaderState(true);
            }
        }, null);
        m_taskQueue.offer(ft);
        try {
            ft.get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public InitiatorMailbox(int partitionId,
            Scheduler scheduler,
            HostMessenger messenger, RepairLog repairLog,
            RejoinProducer rejoinProducer)
    {
        m_partitionId = partitionId;
        m_scheduler = scheduler;
        m_messenger = messenger;
        m_repairLog = repairLog;
        m_rejoinProducer = rejoinProducer;

        m_masterLeaderCache = new LeaderCache(m_messenger.getZK(), VoltZK.iv2masters);
        try {
            m_masterLeaderCache.start(false);
        } catch (InterruptedException ignored) {
            // not blocking. shouldn't interrupt.
        } catch (ExecutionException crashme) {
            // this on the other hand seems tragic.
            VoltDB.crashLocalVoltDB("Error constructiong InitiatorMailbox.", false, crashme);
        }
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        m_taskQueue.take().run();
                    }
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

    public void shutdown() throws InterruptedException
    {
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                try {
                    m_masterLeaderCache.shutdown();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (m_algo != null) {
                    m_algo.cancel();
                }
                m_scheduler.shutdown();
                throw new RuntimeException();
            }
        });
        m_es.shutdown();
        m_es.awaitTermination(356, TimeUnit.DAYS);
    }

    // Change the replica set configuration (during or after promotion)
    public void updateReplicas(final List<Long> replicas)
    {
        FutureTask<Object> ft = new FutureTask<Object>(new Runnable() {
            @Override
            public void run() {
                Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
                // If a replica set has been configured and it changed during
                // promotion, must cancel the term
                if (m_algo != null) {
                    m_algo.cancel();
                }
                m_scheduler.updateReplicas(replicas);
            }
        }, null);
        m_taskQueue.offer(ft);
        try {
            ft.get();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ExecutionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public long getMasterHsId(int partitionId)
    {
        long masterHSId = m_masterLeaderCache.get(partitionId);
        return masterHSId;
    }

    @Override
    public void send(long destHSId, VoltMessage message)
    {
        logTxMessage(message);
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSId, message);
    }

    @Override
    public void send(long[] destHSIds, VoltMessage message)
    {
        logTxMessage(message);
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSIds, message);
    }

    @Override
    public void deliver(final VoltMessage message)
    {
        m_taskQueue.offer(new Runnable() {
            @Override
            public void run() {
                logRxMessage(message);
                if (message instanceof Iv2RepairLogRequestMessage) {
                    handleLogRequest(message);
                    return;
                }
                else if (message instanceof Iv2RepairLogResponseMessage) {
                    m_algo.deliver(message);
                    return;
                }
                else if (message instanceof RejoinMessage) {
                    m_rejoinProducer.deliver((RejoinMessage)message);
                    return;
                }
                m_repairLog.deliver(message);
                m_scheduler.deliver(message);
            }
        });
    }

    @Override
    public VoltMessage recv()
    {
        return null;
    }

    @Override
    public void deliverFront(VoltMessage message)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking()
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recv(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s, long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public long getHSId()
    {
        return m_hsId;
    }

    @Override
    public void setHSId(long hsId)
    {
        this.m_hsId = hsId;
    }

    /** Produce the repair log. This is idempotent. */
    void handleLogRequest(VoltMessage message)
    {
        Iv2RepairLogRequestMessage req = (Iv2RepairLogRequestMessage)message;
        List<Iv2RepairLogResponseMessage> logs = m_repairLog.contents(req.getRequestId(),
                req.isMPIRequest());

        tmLog.info(""
            + CoreUtils.hsIdToString(getHSId())
            + " handling repair log request id " + req.getRequestId()
            + " for " + CoreUtils.hsIdToString(message.m_sourceHSId) + ". ");

        for (Iv2RepairLogResponseMessage log : logs) {
            send(message.m_sourceHSId, log);
        }
    }

    /**
     * Create a real repair message from the msg repair log contents and
     * instruct the message handler to execute a repair. Single partition
     * work needs to do duplicate counting; MPI can simply broadcast the
     * repair to the needs repair units -- where the SP will do the rest.
     */
    void repairReplicasWith(List<Long> needsRepair, VoltMessage repairWork)
    {
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work = new Iv2InitiateTaskMessage(getHSId(), getHSId(), m);
            m_scheduler.handleIv2InitiateTaskMessageRepair(needsRepair, work);
        }
        else if (repairWork instanceof CompleteTransactionMessage) {
            send(com.google.common.primitives.Longs.toArray(needsRepair), repairWork);
        }
        else {
            throw new RuntimeException("Invalid repair message type: " + repairWork);
        }
    }

    void logRxMessage(VoltMessage message)
    {
        Iv2Trace.logInitiatorRxMsg(message, m_hsId);
        if (LOG_RX) {
            hostLog.info("RX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }

    void logTxMessage(VoltMessage message)
    {
        if (LOG_TX) {
            hostLog.info("TX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }
}
