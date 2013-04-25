package nl.vu.cs.ajira.datalayer.files;

import java.io.File;
import java.io.IOException;
import java.util.NoSuchElementException;

import nl.vu.cs.ajira.actions.ReadFromFiles;
import nl.vu.cs.ajira.data.types.Tuple;

/**
 * A <code>FileReader</code> in Ajira is basically an iterator over tuples obtained
 * from a file. There is a default implementation available (through
 * {@link DefaultFileReader}), but users can supply their own. See
 * {@link ReadFromFiles#S_CUSTOM_READER}.
 */
public interface FileReader {

	/**
	 * Determines whether another tuple is available.
	 * @return
	 * 			<code>true</code> if another tuple is available, <code>false</code> otherwise.
	 * @throws IOException
	 * 			in case of trouble
	 */
	public boolean next() throws IOException;

	/**
	 * Obtains the next tuple (if available) and copies it into the parameter.
	 * @param tuple
	 * 			destination where the tuple will be copied to.
	 * @throws IOException
	 * 			in case of trouble
	 * @throws NoSuchElementException
	 *          when no tuple is available.
	 */
	public void getTuple(Tuple tuple) throws IOException;

	/**
	 * Initializes this <code>FileReader</code>.
	 * @param file
	 * 			the file to be read
	 * @throws IOException
	 * 			in case of trouble (for instance when the file could not be opened)
	 */
	public void init(File file) throws IOException;

	/**
	 * Closes this <code>FileReader</code>, and releases underlying
	 * resources.
	 */
	public void close();
}
