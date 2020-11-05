package cn.itcast.avro;

import cn.itcast.config.QuotConfig;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @Date 2020/9/15
 */
//0.创建AvroDeserializeSchema<T> implements DeserializationSchema<T>
public class AvroDeserializeSchema<T> implements DeserializationSchema<T> {
    //1.定义成员变量topicName
    private String topicName;

    //2.创建带参构造topicName
    public AvroDeserializeSchema(String topicName) {
        this.topicName = topicName;
    }

    //3.重写数据反序列化方法deserialize
    @Override
    public T deserialize(byte[] message) throws IOException {
        SpecificDatumReader<T> specificDatumReader = null;
        if (topicName.equals(QuotConfig.config.getProperty("sse.topic"))) {
            //定义规范
            specificDatumReader = new SpecificDatumReader(SseAvro.class);
        } else {
            specificDatumReader = new SpecificDatumReader(SzseAvro.class);
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(message);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bis, null);
        T read = specificDatumReader.read(null, binaryDecoder);
        return read;
    }
    //4.重写获取反序列化类型方法getProducedType
    @Override
    public TypeInformation<T> getProducedType() {
        TypeInformation<T> typeInformation = null;
        if (topicName.equals(QuotConfig.config.getProperty("sse.topic"))) {
            typeInformation = (TypeInformation<T>) TypeInformation.of(SseAvro.class);
        } else {
            typeInformation = (TypeInformation<T>) TypeInformation.of(SzseAvro.class);
        }
        return typeInformation;
    }
    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }
}
