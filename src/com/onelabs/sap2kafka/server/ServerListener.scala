package com.onelabs.sap2kafka.server

import com.sap.conn.jco.server.{JCoServer, JCoServerContextInfo,
  JCoServerErrorListener, JCoServerExceptionListener, JCoServerState,
  JCoServerStateChangedListener}

/**
 * This example shows the three different listeners which can be added to {@link JCoServer}. You can add a
 * {@link JCoServerErrorListener} and {@link JCoServerExceptionListener} to track problems. The
 * {@link JCoServerStateChangedListener} informs about {@link JCoServerState} changes.
 *
 * To see an example error you can force a disconnect from the gateway monitor SMGW. Under clients, you can delete the
 * respective connections. You will notice, that the listener prints out the errors on the console.
 */
object ServerListener {
  def main(args: Array[String]): Unit = {
    val server = KafkaProducerFunctionHandling.createServer
    KafkaProducerFunctionHandling.registerFunctionHandler(server)
    server.addServerErrorListener(MyServerFailureErrorListener)
    server.addServerExceptionListener(MyServerExceptionListener)
    server.addServerStateChangedListener(MyStateChangedListener)
    server.start()
    System.out.println("The program can be stopped using <ctrl>+<c>")
  }

  object MyServerFailureErrorListener extends JCoServerErrorListener {
    override def serverErrorOccurred(jcoServer: JCoServer, connectionId: String, serverCtx: JCoServerContextInfo, error: Error): Unit = {
      System.out.println(">>> Error occurred on " + jcoServer.getProgramID + " connection " + connectionId)
      error.printStackTrace()
    }
  }

  object MyServerExceptionListener extends JCoServerExceptionListener {
    override def serverExceptionOccurred(jcoServer: JCoServer, connectionId: String, serverCtx: JCoServerContextInfo, ex: Exception): Unit = {
      System.out.println(">>> Exception occurred on " + jcoServer.getProgramID + " connection " + connectionId)
      ex.printStackTrace()
    }
  }

  object MyStateChangedListener extends JCoServerStateChangedListener {
    override def serverStateChangeOccurred(server: JCoServer, oldState: JCoServerState, newState: JCoServerState): Unit = { // Defined states are: STARTED, DEAD, ALIVE, STOPPED, HA_BROKEN; see JCoServerState class for details.
      // Details for connections managed by a server instance are available via JCoServerMonitor.
      System.out.println("Server state changed from " + oldState + " to " + newState + " on server with program id " + server.getProgramID)
    }
  }

}
