package cn.itcast.avro;

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
 * Date 2020/10/23 12:01
 * Desc 测试Avro
 * 序列化:将Java对象-->二进制
 * 反序列化:二进制-->Java对象
 */
public class TestAvroSerializeAndDeSerialize {
    public static void main(String[] args) throws IOException {
        //1.准备Java对象
        User user1 = new User("tom", 20, "shanghai");

        User user2 = new User();
        user2.setName("jack");
        user2.setAge(18);
        user2.setAddress("beijin");

        User user3 = User.newBuilder()
                .setName("tony")
                .setAge(19)
                .setAddress("shenzhen")
                .build();

        //2.序列化--将Java对象转为二进制数据存到users.avro文件中了
        //-创建写标准对象
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        //-创建写文件对象
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        //-设置文件路径和Schema
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        //-设置要写哪些对象
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        //3.反序列化--将users.avro文件中的二进制数据反序列化为Java对象
        //-创建读标准对象
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        //-创建读文件对象
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("users.avro"), userDatumReader);
        //-循环读
        User user = null;
        while (dataFileReader.hasNext()) {
            //传进入的是空的user,返回来的是反序列化之后的user
            user = dataFileReader.next(user);
            System.out.println("反序列化出来的Java对象信息为:" + user);
        }

    }
}
