package com.onelabs.sap2kafka.server

/**
 * Similar to {@link DestinationDataProvider}, JCo deals with with server data like SERVER_NAME1 which separates the application
 * logic from technical configuration. JCo retrieves server data information from the {@link ServerDataProvider}. For these test
 * examples we use the given jcoServer file in this package. Replace the dummy values and put the file in the working directory.
 * Please don't use it in a productive environment, but register your own implementation of {@link ServerDataProvider}.
 */
object ServerNameConcept {

  object SomeSampleServers {
    val SERVER_NAME1 = "EXT_SERVER"
    val SERVER_NAME2 = "EXT_HA_SERVER"
    // public static final String SERVER_NAME3 = "<put any name for the server here>";
  }

}
