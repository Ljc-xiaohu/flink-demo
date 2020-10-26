package cn.itcast.demo;

import cn.itcast.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Author itcast
 * Date 2020/9/29 20:34
 * Desc
 */
public class AvroDemo {
    public static void main(String[] args) throws IOException {
        //1.新建对象
        User user1 = new User();
        //2.设置数据
        user1.setName("jack");
        user1.setAge(18);
        user1.setAddress("杭州");

        User user2 = new User("robin", 19, "北京");

        User user3 = User.newBuilder()
                .setName("pony")
                .setAge(20)
                .setAddress("深圳")
                .build();

        //3.序列化
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        //4.反序列化
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("users.avro"), userDatumReader);
        User user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
