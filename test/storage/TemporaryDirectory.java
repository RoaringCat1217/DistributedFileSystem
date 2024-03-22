package test.storage;

import java.io.*;
import java.lang.ref.*;

/** Temporary directories for testing.

    <p>
    Temporary directories are created under the directory given by the system
    property <code>java.io.tmpdir</code>.

    <p>
    Temporary directories must be removed manually by calling <code>remove</code>.
 */
public class TemporaryDirectory {
    /** <code>File</code> object representing the directory. */
    private final File directory;

    /** Creates a temporary directory.

        @throws FileNotFoundException If a directory cannot be created. This can
                                      occur due to permissions problems, or due
                                      to the exhaustion of temporary directory names.
     */
    public TemporaryDirectory(String dir) throws FileNotFoundException {
        File attempt = new File(dir);

        if (attempt.mkdir()) {
            directory = attempt;
            return;
        }

        throw new FileNotFoundException("unable to create temporary directory " + attempt.toString());
    }
    
    /** Retrieves the <code>File</code> object representing the temporary directory.
        @return The <code>File</code> object.
     */
    public File root() {
        return directory;
    }

    /** Recursively deletes a directory.

        @param file The directory to be deleted.
        @return <code>true</code> if the directory is successfully deleted, and
                false otherwise.
     */
    private boolean deleteRecursive(File file) {
        if(file.isDirectory()) {
            for(String child : file.list()) {
                if(!deleteRecursive(new File(file, child)))
                    return false;
            }
        }
        return file.delete();
    }

    /** Removes a temporary directory and all contents / sub-directories. */
    public synchronized void remove() {
        deleteRecursive(directory);
    }

    /** Adds a file (and all needed directories) to the temporary directory.

        @param path The path to the file.
        @throws IllegalArgumentException If <code>path</code> represents the
                                         temporary directory itself.
        @throws IOException If the file cannot be created.
     */
    public void add(String[] path) throws IOException, IllegalArgumentException {
        if(path.length < 1)
            throw new IllegalArgumentException("path is the root directory");

        // Find or create the directory in which the file will be located.
        File current_directory = directory;

        for(int index = 0; index < path.length - 1; ++index) {
            current_directory = new File(current_directory, path[index]);
            current_directory.mkdir();
            if(!current_directory.isDirectory()) {
                throw new IOException("path component " + path[index] + " is " +
                                      "not a directory or cannot be created");
            }
        }

        // Create the file.
        File file = new File(current_directory, path[path.length - 1]);
        if(!file.createNewFile())
            throw new IOException("unable to create file " + path[path.length-1]);
    }
}
