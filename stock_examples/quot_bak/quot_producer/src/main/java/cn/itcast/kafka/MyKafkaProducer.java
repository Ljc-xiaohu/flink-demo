package cn.itcast.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Author itcast
 * Desc
 */
//1.创建自定义Kafka生产者对象MyKafkaProducer<T extends SpecificRecordBase>
public class MyKafkaProducer<T extends SpecificRecordBase> {
    //2.创建KafkaProducer:new KafkaProducer<>(getProperties());
    KafkaProducer<Object, Object> kafkaProducer = new KafkaProducer<>(getProperties());
    //3.获取配置参数,注意value序列化使用自定义的AvroSerializerSchema.class.getName()
    private Properties getProperties() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.52.100:9092");
        prop.setProperty("acks","1");
        prop.setProperty("retries","0");
        prop.setProperty("batch.size","16384");
        prop.setProperty("linger.ms","1");
        prop.setProperty("buffer.memory","33554432");
        prop.setProperty("key.serializer",StringSerializer.class.getName());
        prop.setProperty("value.serializer", AvroSerializerSchema.class.getName());
        return prop;
    }
    //4.提供发送数据的方法sendData(String topic,T data){}
    public void sendData(String topic,T data){
        kafkaProducer.send(new ProducerRecord<>(topic,data));
    }
}
