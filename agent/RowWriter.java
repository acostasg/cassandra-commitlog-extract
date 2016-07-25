package agent;

public interface RowWriter {
    public void init(String tableName) throws Exception;

    public void execute(long timestamp) throws Exception;

    public void write(RowBlock block, Filter filter) throws Exception;

    public void commit() throws Exception;

    public void close() throws Exception;
}
