package cn.itcast.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Author itcast
 * Date 2020/10/23 17:05
 * Desc
 * 自定义一个MyKafkaProducer类,用来将数据发送到Kafka,并需要支持序列化
 * 注意:使用泛型类和泛型方法,因为这样更加通用,不光可以发深市数据,以后也可以发沪市数据,只要继承自SpecificRecordBase即可
 */
public class MyKafkaProducer<T extends SpecificRecordBase> {
    private KafkaProducer kafkaProducer = new KafkaProducer<String,SpecificRecordBase>(getPropertie());

    private Properties getPropertie() {
       Properties props = new Properties();
       //设置Kafka连接参数
       props.setProperty("bootstrap.servers","192.168.52.100:9092");
       props.setProperty("acks","1"); //1表示需要leader确认 0表示不需要确认 -1/all表示需要leader和isr都都让
       props.setProperty("retries","0");//重试次数
       props.setProperty("batch.size","16384");//批大小
       props.setProperty("linger.ms","1");//发送时间间隔
       props.setProperty("buffer.memory","33554432");//buffer缓存大小
       props.setProperty("key.serializer", StringSerializer.class.getName());//key序列化
       props.setProperty("value.serializer",AvroSerializerSchema.class.getName());//Value序列化,也就是对发送的数据进行序列化,需要使用Avro的序列化
       return props;
    }


    public void sendData(String topic,T data){
        kafkaProducer.send(new ProducerRecord<>(topic,data));
    }
}
