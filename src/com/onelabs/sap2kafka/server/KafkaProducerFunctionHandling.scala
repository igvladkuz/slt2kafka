package com.onelabs.sap2kafka.server

import java.io.FileReader
import java.util
import java.util.{Hashtable, Map, Properties}

import com.sap.conn.jco.{JCoException, JCoFunction}
import com.sap.conn.jco.server._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

object KafkaProducerFunctionHandling {
  val FUNCTION_NAME = "Z_REPL_KAFKA_JSON_SEND"

  def main(args: Array[String]): Unit = {
    val server = createServer
    registerFunctionHandler(server)
    server.start
    System.out.println("The program can be stopped using <ctrl>+<c>")
  }

  def createServer: JCoServer = try JCoServerFactory.getServer(ServerNameConcept.SomeSampleServers.SERVER_NAME1)
  catch {
    case ex: JCoException =>
      throw new RuntimeException("Unable to create the server " + ServerNameConcept.SomeSampleServers.SERVER_NAME1 + " because of " + ex.getMessage, ex)
  }

  def registerFunctionHandler(server: JCoServer): Unit = {
    val factory = new DefaultServerHandlerFactory.FunctionHandlerFactory
    factory.registerHandler(FUNCTION_NAME, KafkaProducerHandler)
    server.setCallHandlerFactory(factory)
  }

  /**
   * This class provides the implementation for the function FUNCTION_NAME.
   */
  object KafkaProducerHandler extends JCoServerFunctionHandler {
    val IN_PARAM = "MSG"
    val OUT_PARAM = "RESP"
    //static Producer<String, String> producer = null;
    private[server] val TOPIC_NAME = "test"
    //public static final int NUM_THREADS = 1;
    private[server] val myTIDHandler: KafkaProducerFunctionHandling.MyTIDHandler = null

    private def createProducer: KafkaProducer[Long,String] = try {
      val properties = new Properties()
      properties.load(new FileReader("resources/producer.config"))
      properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
      new KafkaProducer(properties)
    } catch {
      case e: Exception =>
        System.out.println("Failed to create producer with exception: " + e)
        System.exit(0)
        null //unreachable
    }

    override def handleRequest(serverCtx: JCoServerContext, function: JCoFunction): Unit = {
      System.out.println("----------------------------------------------------------------")
      System.out.println("Called function   : " + function.getName)
      System.out.println("ConnectionId      : " + serverCtx.getConnectionID)
      System.out.println("SessionId         : " + serverCtx.getSessionID)
      System.out.println("Repository name   : " + serverCtx.getRepository.getName)
      System.out.println("Is in transaction : " + serverCtx.isInTransaction)
      System.out.println("TID               : " + serverCtx.getTID)
      System.out.println("Is stateful       : " + serverCtx.isStatefulSession)
      System.out.println("----------------------------------------------------------------")
      System.out.println("Gateway host      : " + serverCtx.getServer.getGatewayHost)
      System.out.println("Gateway service   : " + serverCtx.getServer.getGatewayService)
      System.out.println("Program ID        : " + serverCtx.getServer.getProgramID)
      System.out.println("----------------------------------------------------------------")
      System.out.println("Attributes        : ")
      System.out.println(serverCtx.getConnectionAttributes.toString)
      System.out.println("----------------------------------------------------------------")
      System.out.println("CPIC conversation ID: " + serverCtx.getConnectionAttributes.getCPICConversationID)
      System.out.println("----------------------------------------------------------------")
      System.out.println("Parameter: " + function.getImportParameterList.getString(KafkaProducerHandler.IN_PARAM))
      val message = function.getImportParameterList.getString(KafkaProducerHandler.IN_PARAM)
      // In sample 3 (tRFC Server) we also set the status to executed:
      if (KafkaProducerHandler.myTIDHandler != null) KafkaProducerHandler.myTIDHandler.execute(serverCtx)
      //
      //            // Assign topicName to string variable
      //            String topicName = TOPIC_NAME;
      //            // create instance for properties to access producer configs
      //            Properties props = new Properties();
      //            // Assign localhost id
      //            props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
      //            // Set acknowledgements for producer requests.
      //            props.put("acks", "all");
      //            // If the request fails, the producer can automatically retry,
      //            props.put("retries", 0);
      //            // Specify buffer size in config
      //            props.put("batch.size", 16384);
      //            // Reduce the no of requests less than 0
      //            props.put("linger.ms", 1);
      //            // The buffer.memory controls the total amount of memory available to the
      //            // producer for buffering.
      //            props.put("buffer.memory", 33554432);
      //            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      //            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      //            producer = new KafkaProducer<String, String>(props);
      //            producer.send(new ProducerRecord<String, String>(topicName, message, message));
      //Create Kafka Producer
      val producer = KafkaProducerHandler.createProducer
      val time = System.currentTimeMillis
      val record = new ProducerRecord[Long, String](KafkaProducerHandler.TOPIC_NAME, time, message)
      producer.send(record, (metadata: RecordMetadata, exception: Exception) => if (exception != null) {
        System.out.println(exception)
        System.exit(1)
      })
      function.getExportParameterList.setValue(KafkaProducerHandler.OUT_PARAM, "Message sent successfully")
      System.out.println("Message sent successfully")
    }
  }

  private[server] object MyTIDHandler {

    private object TIDState extends Enumeration {
      type TIDState = Value
      val CREATED, EXECUTED, COMMITTED, ROLLED_BACK, CONFIRMED = Value
    }

  }

  private[server] class MyTIDHandler extends JCoServerTIDHandler {
    private[server] val availableTIDs = new util.Hashtable[String, Object]

    def checkTID(serverCtx: JCoServerContext, tid: String): Boolean = { // This example uses a Hashtable to store status information. Normally, however,
      // you would use a database. If the DB is down throw a RuntimeException at
      // this point. JCo will then abort the tRFC and the R/3 backend will try
      // again later.
      System.out.println("TID Handler: checkTID for " + tid)
      val state = availableTIDs.get(tid)
      if (state == null) {
        availableTIDs.put(tid, KafkaProducerFunctionHandling.MyTIDHandler.TIDState.CREATED)
        return true
      }
      if ((state eq KafkaProducerFunctionHandling.MyTIDHandler.TIDState.CREATED) || (state eq KafkaProducerFunctionHandling.MyTIDHandler.TIDState.ROLLED_BACK)) return true
      false
      // "true" means that JCo will now execute the transaction, "false" means
      // that we have already executed this transaction previously, so JCo will
      // skip the handleRequest() step and will immediately return an OK code to R/3.
    }

    def commit(serverCtx: JCoServerContext, tid: String): Unit = {
      System.out.println("TID Handler: commit for " + tid)
      // react on commit, e.g. commit on the database;
      // if necessary throw a RuntimeException, if the commit was not possible
      availableTIDs.put(tid, KafkaProducerFunctionHandling.MyTIDHandler.TIDState.COMMITTED)
    }

    def rollback(serverCtx: JCoServerContext, tid: String): Unit = {
      System.out.println("TID Handler: rollback for " + tid)
      availableTIDs.put(tid, KafkaProducerFunctionHandling.MyTIDHandler.TIDState.ROLLED_BACK)
      // react on rollback, e.g. rollback on the database
    }

    def confirmTID(serverCtx: JCoServerContext, tid: String): Unit = {
      System.out.println("TID Handler: confirmTID for " + tid)
      try {
      } finally availableTIDs.remove(tid)
    }

    def execute(serverCtx: JCoServerContext) = {
      val tid = serverCtx.getTID
      if (tid != null) {
        System.out.println("TID Handler: execute for " + tid)
        availableTIDs.put(tid, KafkaProducerFunctionHandling.MyTIDHandler.TIDState.EXECUTED)
      }
    }
  }

}