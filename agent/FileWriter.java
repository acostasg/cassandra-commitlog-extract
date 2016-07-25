package agent;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;

public class FileWriter {
	
	protected static Logger logger = Logger.getLogger(FileWriter.class.getName());

    private String fileName;
    private String extension;
    private long fragmentSize;

    private int fragment;
    private long bytes;
    private OutputStream out;

    public FileWriter(String fileName, String extension, long fragmentSize) throws Exception {
        this.fileName = fileName;
        this.extension = extension;
        this.fragmentSize = fragmentSize;
        fragment = 0;
        bytes = 0;
        out = null;
        newFragment();
    }

    private void newFragment() throws Exception {
        close();
        fragment++;
        String name = new String();
        name = fileName + "_" + String.format("%05d", fragment) + "." + extension;
        out = new FileOutputStream(name, true);        
        bytes = 0;
        logger.debug(" The create file in "+ name);
    }

    private void prepareFragment() throws Exception {
        if (bytes > fragmentSize)
            newFragment();
    }

    public void close() throws Exception {
        if (out != null)
            out.close();
        out = null;
    }

    public void write(String s) throws Exception {
        prepareFragment();
        byte[] b = s.getBytes();
        out.write(b);
        bytes += b.length;
    }
    
    public String getFileName() throws Exception {
    	return this.fileName;
    }    
    
    public String getExtension() throws Exception {
    	return this.extension;
    }    

}
