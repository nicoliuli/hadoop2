package hive.day01;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 自定义udf函数，求字符串的长度
 */
public class MyUdfStrLen extends GenericUDF {

    // 初始化，校验参数是否合法
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("长度不为1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    // 业务逻辑函数，入参表示自定义函数的参数
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String input = deferredObjects[0].get().toString();
        return input.length();
    }

    public String getDisplayString(String[] strings) {
        return null;
    }
}

/*
自定义udf函数过程：
1、引入hive依赖，并编写如上代码
2、mvn clean package打包
3、将打好的jar包上传到$HIVE_HOME/lib下，或直接进入hive命令行，执行 add jar /Users/liuli/code/hadoop2/target/hadoop2-1.0-SNAPSHOT.jar
4、进入hive的命令行，执行
    create [temporary] function mylen as "hive.day01.MyUdfStrLen";
    temporary表示是临时生效。

5、使用
    select mylen(name) from user;
 */
