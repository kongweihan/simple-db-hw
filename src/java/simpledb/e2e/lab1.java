package simpledb.e2e;

import simpledb.*;

import java.io.*;

/**
 * How to run:
 * 1) convert data: java -jar dist/simpledb.jar convert test/simpledb/e2e/lab1.txt 3
 * 2) build:        ant
 * 3) run:          java -classpath dist/simpledb.jar simpledb.e2e.lab1
 */
public class lab1 {
    public static void main(String[] args) {
        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("test/simpledb/e2e/lab1.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
//        System.out.println(table1.numPages());
//        System.out.println(table1.getTupleDesc());

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
//        System.out.println(f.getTupleDesc());


        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }
}
