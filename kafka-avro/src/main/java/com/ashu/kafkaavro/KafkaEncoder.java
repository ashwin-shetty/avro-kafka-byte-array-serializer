package com.ashu.kafkaavro;


import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KafkaEncoder {


    static <T extends SpecificRecord> T decode(ConsumerRecord<String, byte[]> record, Class<T> clazz) {
            DatumReader<T> deserializeDatumReader = new SpecificDatumReader<>(clazz);
            Decoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null);
            T incomingMsg=null;
            try {
                incomingMsg = deserializeDatumReader.read(null, decoder);
            } catch (IOException ignored) {
            }
        return incomingMsg;
    }

    static <T extends SpecificRecord> byte[] encode(T data) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DatumWriter<T> outputDatumWriter = new SpecificDatumWriter<>(data.getSchema());
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
            outputDatumWriter.write(data, encoder);
            encoder.flush();
            return baos.toByteArray();
        } catch (Exception ex) {
           ex.printStackTrace();
        }
        return null;
    }


}