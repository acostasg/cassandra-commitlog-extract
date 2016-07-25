package agent;

import java.sql.BatchUpdateException;

import org.apache.log4j.Logger;

public class PostgresWriterJDBC extends PostgresWriter implements RowWriter {

    protected static Logger logger = Logger.getLogger(PostgresWriterJDBC.class.getName());

    private JDBCWriter writer;

    public void init(String table) throws Exception {
        this.table = table;
        writer = new JDBCWriter("postgres");
        writer.write(initTempTable());
    }

    public void write(RowBlock block, Filter filter) throws Exception {
        writer.write(insertsToTemp(block, filter));
    }

    public void close() throws Exception {
        writer.close();
    }

    public void commit() throws Exception {
        writer.write(mergeTempTable());
    }

    @Override
    public void execute(long timestamp) throws Exception {
        // TODO Auto-generated method stub

    }

}
