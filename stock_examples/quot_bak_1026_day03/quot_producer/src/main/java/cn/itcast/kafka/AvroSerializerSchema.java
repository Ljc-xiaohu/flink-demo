package cn.itcast.kafka;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Author itcast
 * Desc
 */
//1.自定义Avro序列化对象AvroSerializerSchema<T extends SpecificRecordBase> implements Serializer<T>
public class AvroSerializerSchema<T extends SpecificRecordBase> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }
    //2.重写serialize方法
    @Override
    public byte[] serialize(String topic, T data) {
        //2.1.创建字节数组输出流对象bos=new ByteArrayOutputStream()
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        //2.2创建SpecificDatumWriter对象
        SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        //2.3.创建编码器EncoderFactory.get().directBinaryEncoder
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);//null表示不重新使用之前的编码器
        try {
            //2.4写出数据流对象datumWriter.write(data,binaryEncoder)
            datumWriter.write(data,binaryEncoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //2.5返回流对象中的字节数组bos.toByteArray()
        return bos.toByteArray();
    }

    @Override
    public void close() {
    }
}
