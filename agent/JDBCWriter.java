package agent;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.log4j.Logger;
import org.postgresql.util.PSQLException;

import sun.awt.windows.ThemeReader;

public class JDBCWriter {
    protected static Logger logger = Logger.getLogger(JDBCWriter.class.getName());

    private Connection conn;
    private Statement stmt;
    private String url;

    public JDBCWriter(String config) throws Exception {
        Map<String, Object> conf = ConfManager.getConnection(config);
        url = (String) conf.get("url");
        String driver = (String) conf.get("driver");
        String user = (String) conf.get("user");
        String password = (String) conf.get("password");

        logger.info("Opening new connection to " + url + "...");

        Class.forName(driver);
        conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
        stmt = conn.createStatement();
    }

    public Connection getConnection() {
        return conn;
    }

    public void close() throws Exception {
        try {
            long elapsed = System.currentTimeMillis();

            logger.info("Closing connection to " + url + "...");

            if (stmt != null)
                stmt.close();
            stmt = null;
            if (conn != null)
                conn.close();
            conn = null;

            elapsed = System.currentTimeMillis() - elapsed;
            logger.info("Closing took " + elapsed + "ms");
        } catch (BatchUpdateException e) {
            logger.info(e.getNextException().getSQLState());
            if (e.getSQLState() != null) {
                if (e.getNextException().getSQLState().equals("25P02")) {
                    logger.info("Closing took");
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    public void write(List<String> l) throws Exception {
        long elapsed = System.currentTimeMillis();
        String sample = "";

        stmt.clearBatch();

        for (String s : l) {
            if (sample.length() < 50) {
                //logger.info("sql: " + s);
                sample = sample + s.substring(0, 10) + "...,";
            }
            stmt.addBatch(s);
        }

        try {
            boolean committed = false;
            boolean deadlocked = false;

            synchronized (conn) {
                do {
                    deadlocked = false;
                    try {
                        stmt.executeBatch();
                        conn.commit();
                        committed = true;
                    } catch (BatchUpdateException e) {
                        logger.info("***** Table Exception *******" + e.getMessage().toString());
                        if (e.getNextException() != null) {
                            if (e.getNextException().getSQLState().equals("40P01") || e.getNextException().getSQLState().equals("ORA-00060")) {
                                /* Log the fact that we're deadlocked */
                                logger.info("***** Table Deadlocked true , waiting 1 minut *******" + e.getNextException().getSQLState().toString());
                                wait(1000);
                                deadlocked = true;
                            } else {
                                throw e.getNextException();
                            }
                        } else {
                            throw e;
                        }
                    } finally {
                        try {
                            if (!committed) {
                                conn.rollback();
                            }
                        } catch (SQLException e) {
                            throw e;
                        }
                    }
                } while (deadlocked);
            }

        } catch (Exception e) {
            throw e;
        }

        elapsed = System.currentTimeMillis() - elapsed;
        logger.info("Batching " + l.size() + " elements (" + sample + ") took " + elapsed + "ms");
    }

}
