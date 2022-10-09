package com.example;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.List;
import java.nio.ByteBuffer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;

/**
 * Hello world!
 */
public final class App {

	static CqlSession _Session;

    private App() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
         ConnectToCluster();

         List<Row> rows = GetPartition("testkey");

         ClonePartition(rows, 1);
    }

    private static void ConnectToCluster() {
		// create a cluster instance

        _Session = CqlSession.builder()
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .withLocalDatacenter("SearchGraph")
            .withKeyspace("athena")
            .build();
    // CqlSession _Session = CqlSession.builder().build();
		
		// Open a session with the cluster
	    ResultSet rs = _Session.execute("select release_version from system.local");              // (2)
        Row row = rs.one();
        System.out.println(row.getString("release_version"));                                    // (3)

	}

    public static List<Row> GetPartition(String partitionKey) {
        String command = "select * from athena.raw_agg where key='" + partitionKey + "'";
        System.out.println(command);
        ResultSet rs = _Session.execute(command);              // (2)
        List<Row> rows = rs.all();
        System.out.println("The total # of rows returned: " + rows.size());

        for (int i=0; i< rows.size(); i++) {
            
            if (i == 1) {
                Blob_table blob = new Blob_table(
                    rows.get(i).getString("key"),
                    rows.get(i).getUuid("column1"),
                    rows.get(i).getByteBuffer("value")
                );
                blob.print();
            
            }
        }

        return rows;

    }

    public static void ClonePartition(List<Row> rows, int numberOfClones) {

        String newKey;
        Blob_table blob;

        for (int i=0; i < numberOfClones; i++) {
            newKey = "testkey" + i;

            for (int j=0; j < rows.size(); j++) {
                blob = new Blob_table(
                    newKey,
                    rows.get(j).getUuid("column1"),
                    rows.get(j).getByteBuffer("value")
                );
                InsertBlobRow(blob);                
            }

        }
    }

    public static void InsertBlobRow(String key, UUID column1, ByteBuffer value) {
        System.out.println("Writing blow row ...");
        Insert query = QueryBuilder.insertInto("raw_agg")
            .value("key", key)
            .value("column1", column1)
            .value("value", value);
        
        _Session.execute(query);
    }

    public static void InsertBlobRow(Blob_table blob) {
        System.out.print("Writing blow row ... ");
        blob.print();


        // Insert query = QueryBuilder.insertInto("raw_agg")
        //     .value("key", blob.getKey())
        //     // .value("column1", blob.getColumn1())
        //     // .value("value", blob.getValue())
        // ;
        
        // _Session.execute(query);

        SimpleStatement statement = SimpleStatement.newInstance("INSERT INTO Athena.raw_agg (key, column1, value) VALUES (?,?,?)", blob.getKey(),blob.getColumn1(), blob.getValue());
        _Session.execute(statement);
        
    }
}
