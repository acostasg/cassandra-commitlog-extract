package agent;

import java.io.*;
import java.util.*;
import java.security.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;

public class Utils {

    protected static Logger logger = Logger.getLogger(Main.class.getName());

    private static MessageDigest md5Digest = null;
    private static Map<String, String> columnCache = null;

    // obfuscate some column names to workaround Oracle column name limits
    public static Map<String, String> validOracleColumnNames(Set<String> cols) {
        if (Utils.md5Digest == null) {
            try {
                Utils.md5Digest = MessageDigest.getInstance("MD5");
            } catch (Exception e) {

            }
        }
        if (Utils.columnCache == null) {
            Utils.columnCache = new HashMap<String, String>();
        }

        Map<String, String> r = new HashMap<String, String>();
        for (String col : cols) {
            String validCol = col;
            if (Utils.columnCache.containsKey(col)) {
                validCol = columnCache.get(col);
            } else {
                if (col.length() > 30) {
                    byte[] colBytes = null;
                    try {
                        colBytes = col.getBytes("UTF-8");
                    } catch (Exception e) {
                        //log DEBUG
                        logger.debug("Exception column check Cassandra:" + col);
                    }
                    byte[] colMD5 = null;
                    colMD5 = Utils.md5Digest.digest(colBytes);
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < 4; i++) {
                        sb.append(String.format("%02X", colMD5[i]));
                    }
                    validCol = col.substring(0, 22) + sb.toString();
                }
                columnCache.put(col, validCol);
            }
            r.put(col, validCol);
        }

        //log DEBUG list validates col
        logger.debug("Mapping validate columns:" + r.toString());

        return r;
    }

    public static String deFux(String v) throws Exception {
        // remove mapper JSON header
        if (v.startsWith("!!!Groupalia_Mapper_Manager.JSON!!!", 0)) {
            v = v.substring(35);
        }

        // there is a lot of crap in this database. nuke some of it, we cannot search it anyway
        byte[] b = v.getBytes("ISO-8859-1");
        boolean changed = false;
        for (int i = 0; i < b.length; i++) {
            // remove useless control chars
            if ((b[i] >= 0 && b[i] < 32) && b[i] != 13 && b[i] != 10 && b[i] != 9) {
                b[i] = 32;
                changed = true;
            }
            // ^ - we reserve this char for the COPY delimiter
            // " - it fucks up quoting
            // \ - it fucks up quoting
            if (b[i] == '^' || b[i] == '"' || b[i] == '\\') {
                b[i] = 32;
                changed = true;
            }
        }
        if (changed) {
            v = new String(b, "ISO-8859-1");
        }
        v = Utils.fixEncoding(v);
        return v;
    }

    public static String deFuxOracle(String v) throws Exception {
        // remove mapper JSON header
        if (v.startsWith("!!!Groupalia_Mapper_Manager.JSON!!!", 0)) {
            v = v.substring(35);
        }
        // remplace ' for ''
        v = v.replaceAll("'", "''");

        // there is a lot of crap in this database. nuke some of it, we cannot search it anyway
        byte[] b = v.getBytes("ISO-8859-1");
        boolean changed = false;
        for (int i = 0; i < b.length; i++) {
            // remove useless control chars
            if ((b[i] >= 0 && b[i] < 32) && b[i] != 13 && b[i] != 10 && b[i] != 9) {
                b[i] = 32;
                changed = true;
            }
            // ^ - we reserve this char for the COPY delimiter
            // \ - it fucks up quoting
            if (b[i] == '^' || b[i] == '\\') {
                b[i] = 32;
                changed = true;
            }
        }
        if (changed) {
            v = new String(b, "ISO-8859-1");
        }
        v = Utils.fixEncoding(v);
        return v;
    }

    public static String deFuxMore(String v) throws Exception {
        // second pass defuxing, because this data is just braindead
        byte[] b = v.getBytes("ISO-8859-1");
        boolean changed = false;
        for (int i = 0; i < b.length; i++) {
            // we won't tolerate control chars now, at all
            if (b[i] >= 0 && b[i] < 32) {
                b[i] = 32;
                changed = true;
            }
        }
        if (changed) {
            v = new String(b, "ISO-8859-1");
        }
        return v;
    }

    public static String hstoreEscape(String v) throws Exception {
        v = v.replaceAll("\\\\", "\\\\");
        v = v.replaceAll("\\\"", "\\\"");
        return v;
    }

    // http://stackoverflow.com/questions/887148/how-to-determine-if-a-string-contains-invalid-encoded-characters
    public static String fixEncoding(String latin1) {
        try {
            byte[] bytes = latin1.getBytes("ISO-8859-1");
            if (!Utils.validUTF8(bytes))
                return latin1;
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // Impossible, throw unchecked
            throw new IllegalStateException("No Latin1 or UTF-8: " + e.getMessage());
        }
    }

    private static boolean validUTF8(byte[] input) {
        int i = 0;
        // Check for BOM
        if (input.length >= 3 && (input[0] & 0xFF) == 0xEF && (input[1] & 0xFF) == 0xBB & (input[2] & 0xFF) == 0xBF) {
            i = 3;
        }

        int end;
        for (int j = input.length; i < j; ++i) {
            int octet = input[i];
            if ((octet & 0x80) == 0) {
                continue; // ASCII
            }

            // Check for UTF-8 leading byte
            if ((octet & 0xE0) == 0xC0) {
                end = i + 1;
            } else if ((octet & 0xF0) == 0xE0) {
                end = i + 2;
            } else if ((octet & 0xF8) == 0xF0) {
                end = i + 3;
            } else {
                // Java only supports BMP so 3 is max
                return false;
            }

            while (i < end) {
                i++;
                if (i >= input.length) {
                    // out of bounds
                    return false;
                }
                octet = input[i];
                if ((octet & 0xC0) != 0x80) {
                    // Not a valid trailing byte
                    return false;
                }
            }
        }
        return true;
    }

}
