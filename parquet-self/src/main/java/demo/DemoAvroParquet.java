package demo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

/**
 * ${DESCRIPTION}
 *
 * @author dengguoqing
 * @date 2019/1/7
 */
public class DemoAvroParquet {

    public static void main(String[] args) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        DemoAvroParquet demoAvroParquet = new DemoAvroParquet();
        Schema schema = parser.parse(
                demoAvroParquet.getClass().getResourceAsStream("StringPair.avsc"));
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", "L");
        datum.put("right", "R");

        Path path = new Path("data1.parquet");
        ParquetWriter<Object> writer = AvroParquetWriter.builder(
                path).withSchema(schema).build();

        writer.write(datum);
        writer.close();

        try (AvroParquetReader<GenericRecord> reader = new AvroParquetReader(path);) {
            GenericRecord result = reader.read();
            System.err.println(result.get("left").toString());
            System.err.println(result.get("right").toString());

        }

    }


}
