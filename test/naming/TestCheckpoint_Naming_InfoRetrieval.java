package test.naming;

import java.io.*;
import java.net.http.HttpResponse;

import common.ExceptionReturn;
import common.PathRequest;
import common.ServerInfo;
import test.common.*;
import test.util.TestFailed;

/** Tests that the naming server <code>get_storage</code> method returns the
    correct storage server info.

    <p>
    Items checked are:
    <ul>
    <li><code>get_storage</code> rejects bad arguments such as <code>null</code>,
        non-existent files, and paths to directories.</li>
    <li><code>get_storage</code> returns address info for storage servers that are
        indeed hosting the file being requested.</li>
    </ul>
 */
public class TestCheckpoint_Naming_InfoRetrieval extends NamingTest {
    /** Test notice. */
    public static final String notice = "checking naming server get_storage method";
    /** Prerequisites. */
    public static final Class[] prerequisites = new Class[] {TestCheckpoint_Naming_Registration.class};

    /** First registering storage server. */
    private TestStorageServer server1;
    /** Second registering storage server. */
    private TestStorageServer server2;

    /** First storage server info. */
    private ServerInfo server1_info;
    /** Second storage server info. */
    private ServerInfo server2_info;

    private final Path file1 = new Path("/file1");
    private final Path file2 = new Path("/directory/file2");

    private final Path file3 = new Path("/directory/file3");
    private final Path file4 = new Path("/another_directory/file4");

    /** Creates the <code>InfoRetrievalTest</code> and sets the notice. */
    public TestCheckpoint_Naming_InfoRetrieval() throws IOException {
        server1 = new TestStorageServer(this);
        server2 = new TestStorageServer(this);
    }

    /** Performs the tests.

        @throws TestFailed If any of the tests fail.
     */
    @Override
    protected void perform() throws TestFailed {
        checkArguments();

        checkInfo(file1, server1_info);
        checkInfo(file2, server1_info);
        checkInfo(file3, server2_info);
        checkInfo(file4, server2_info);
    }

    /** Checks that the naming server returns the correct storage server info
        for the given file.

        @param path The file for which the info is to be requested.
        @param expected_info The info expected to be received.
        @throws TestFailed If the info cannot be retrieved, or if the info
                           retrieved is not the info expected.
     */
    private void checkInfo(Path path, ServerInfo expected_info) throws TestFailed {
        ServerInfo info;
        String exception_type;

        // Try to retrieve the info from the naming server.
        try {
            PathRequest request = new PathRequest(path.toString());
            HttpResponse<String> response = getResponse("/get_storage", service_port, request);
            exception_type = gson.fromJson(response.body(), ExceptionReturn.class).exception_type;
            if(exception_type != null) {
                throw new Throwable(response.body());
            }
            info = gson.fromJson(response.body(), ServerInfo.class);
        } catch(Throwable t) {
            throw new TestFailed("unable to retrieve storage server info for " + path, t);
        }

        if(info == null)
            throw new TestFailed("received null instead of info for " + path);

        // Check that the info received is equal to the info expected.
        if(!info.equals(expected_info))
            throw new TestFailed("received wrong info for " + path);
    }

    /** Checks that the <code>get_storage</code> method rejects bad arguments.

        @throws TestFailed If the test fails.
     */
    private void checkArguments() throws TestFailed {
        // Check that get_storage rejects null.
        try {
            HttpResponse<String> response = getResponse("/get_storage", service_port, new PathRequest(""));
            String exception_type = gson.fromJson(response.body(), ExceptionReturn.class).exception_type;

            if(exception_type == null) {
                throw new TestFailed("get_storage accepted empty string as argument");
            } else if (DFSException.valueOf(exception_type) != DFSException.IllegalArgumentException) {
                throw new Throwable(response.body());
            }
        } catch(TestFailed e) { 
            throw e; 
        } catch(Throwable t) {
            throw new TestFailed("get_storage threw unexpected exception when given null as argument", t);
        }

        // Check that get_storage rejects non-existent files.
        try {
            PathRequest request = new PathRequest("/another_file");
            HttpResponse<String> response = getResponse("/get_storage", service_port, request);
            String exception_type = gson.fromJson(response.body(), ExceptionReturn.class).exception_type;

            if(exception_type == null) {
                throw new TestFailed("get_storage accepted path to non-existent file as argument");
            } else if (DFSException.valueOf(exception_type) != DFSException.FileNotFoundException) {
                throw new Throwable(response.body());
            }
        } catch(TestFailed e) { 
            throw e; 
        } catch(Throwable t) {
            throw new TestFailed("get_storage threw unexpected exception when " +
                                 "given a non-existent file as argument", t);
        }

        // Check that get_storage rejects directories.
        try {
            PathRequest request = new PathRequest("/directory");
            HttpResponse<String> response = getResponse("/get_storage", service_port, request);

            String exception_type = gson.fromJson(response.body(), ExceptionReturn.class).exception_type;

            if(exception_type == null) {
                throw new TestFailed("get_storage accepted directory as argument");
            } else if (DFSException.valueOf(exception_type) != DFSException.FileNotFoundException) {
                throw new Throwable(response.body());
            }
        } catch(TestFailed e) { 
            throw e; 
        } catch(Throwable t) {
            throw new TestFailed("get_storage threw unexpected exception when " +
                                 "given directory as argument", t);
        }
    }

    /** Starts servers used in the test.

        @throws TestFailed If any of the servers cannot be started.
     */
    @Override
    protected void initialize() throws TestFailed {
        super.initialize();

        try {
            server1_info = server1.start(registration_port,
                                         new Path[] {file1, file2}, null);
            server2_info = server2.start(registration_port,
                                         new Path[] {file3, file4, file1},
                                         null);
        } catch(Throwable t) {
            throw new TestFailed("unable to start storage servers", t);
        }
    }

    /** Stops all servers used in the test. */
    @Override
    protected void clean() {
        super.clean();

        if(server1 != null) {
            server1.stop();
            server1 = null;
        }

        if(server2 != null) {
            server2.stop();
            server2 = null;
        }
    }
}
