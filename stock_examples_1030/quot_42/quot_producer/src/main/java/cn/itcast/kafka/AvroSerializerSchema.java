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
 * Date 2020/10/23 17:14
 * Desc 自定义的Avro虚拟化类,用来将SpecificRecordBase转换二进制数据
 */
public class AvroSerializerSchema <T extends SpecificRecordBase> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        //下面的代码表示
        // 先创建一个字节数组输出流对象
        // 再准备一个写标准对象
        // 再创建一个二进制编码器,准备把数据写到字节数组输出流对象之后
        // 再调用写标准对象的write方法,将data使用二进制编码器写到字节数组输出流对象中
        // 最后返回字节数组输出流对象中的二进制数据,也就是byte[]
        //1.创建字节数组输出流对象bos=new ByteArrayOutputStream()
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        //2创建SpecificDatumWriter对象/写标准对象
        SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        //3.创建编码器EncoderFactory.get().directBinaryEncoder
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);//null表示不重新使用之前的编码器
        try {
            //4写出数据流对象datumWriter.write(data,binaryEncoder)
            datumWriter.write(data,binaryEncoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //5返回流对象中的字节数组bos.toByteArray()
        return bos.toByteArray();
    }

    @Override
    public void close() {

    }
}
