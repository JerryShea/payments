package au.com.gritmed.pay;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AvroHelper {
    private final Schema schema;
    private final DatumWriter<GenericRecord> datumWriter;
    private final DatumReader<GenericRecord> datumReader;

    public AvroHelper() throws IOException {
        schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream("payment.avsc"));
        datumWriter = new GenericDatumWriter<>(schema);
        datumReader = new GenericDatumReader<>(schema);
    }

    public GenericRecord getGenericRecord() {
        return new GenericData.Record(schema);
    }

    public void writeToOutputStream(GenericRecord genericRecord, OutputStream outputStream) throws IOException {
        var encoder = EncoderFactory.get().directBinaryEncoder(outputStream, null);
        datumWriter.write(genericRecord, encoder);
    }

    public GenericRecord readFromInputStream(InputStream inputStream) throws IOException {
        var decoder = DecoderFactory.get().directBinaryDecoder(inputStream, null);
        var genericRecord = new GenericData.Record(schema);
        datumReader.read(genericRecord, decoder);
        return genericRecord;
    }
}
