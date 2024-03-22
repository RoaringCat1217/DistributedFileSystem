package test;

/** Commands to run the naming and storage servers for tests
 <p>
 This file contains String definitions that must be modified in
 order for the test code to launch the naming server and multiple
 storage servers.
 
 The command-line arguments included in launching the naming server must 
 include <service-port> and <registration-port> as the
 final two entries in the command, where:
 
 * <service-port> is the integer value used for the naming server's
   listening service port,
 * <registration-port> is the integer value used for the naming server's
   listening registration port.
   
 The command-line arguments included in launching a storage server must
 include <client-port>, <command-port>, <registration-port>, and <root-dir>
 as the final four entries in the command, where:
 
 * <client-port> is the integer value used for the storage server's
   listening client port,
 * <command-port> is the integer value used for the storage server's
   listening command port to receive commands from the naming server,
 * <registration-port> is the integer port number where the storage
   server should contact the naming server for registration purposes
 * <root-dir> is the location of the storage server's root directory
   as a string

 The defined command Strings serve as an example that could be used if 
 the naming and storage servers are implemented in Java.
 </p>
 */
public class ServerCommands {

    /**
     * Test code uses this String to start the naming server as a 
     * new Process.  It also parses the command String to extract the
     * various parameters needed to interact with the naming server.
     *
     * TODO: change this string to start your own naming server using the
     * above specification.
    */
    public static final String namingCommand = 
        "java -cp .:gson-2.8.6.jar naming.NamingServer 8080 8090";

    /**
     * Test code uses this String to start the first storage server
     * as a new Process.  It also parses the command String to extract the
     * various parameters needed to interact with the storage server.
     *
     * TODO: change this string to start your first storage server using
     * the above specification.
    */
    public static final String storage0Command = 
        "java -cp .:gson-2.8.6.jar storage.StorageServer 7000 7001 8090 /tmp/dist-systems-0";

    /**
     * Test code uses this String to start the second storage server
     * as a new Process.  It also parses the command String to extract the
     * various parameters needed to interact with the storage server.
     *
     * TODO: change this string to start your second storage server using
     * the above specification.
    */
    public static final String storage1Command = 
        "java -cp .:gson-2.8.6.jar storage.StorageServer 7010 7011 8090 /tmp/dist-systems-1";
}
