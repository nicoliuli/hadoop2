package hive.day01;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义udtf
 * 输入 aaa,bbb,ccc
 * 输出
 * aaa
 * bbb
 * ccc
 */
public class MyUdtf01 extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 输出数据的默认别名，可以被别名覆盖
        ArrayList<String> fieldNames = new ArrayList();
        // 输出数据的类型，因为炸裂函数不光能把一个数组炸裂成多个行，还能将一个map炸裂成多个列
        ArrayList<ObjectInspector> fieldOIs = new ArrayList();

        fieldNames.add("word");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        /*
        此段代码来自 org.apache.hadoop.hive.ql.udf.generic.GenericUDTFJSONTuple
        for(int i = 0; i < this.numCols; ++i) {
            fieldNames.add("c" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }*/

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 处理输入数据
    public void process(Object[] objects) throws HiveException {
        String input = objects[0].toString();
        String[] words = input.split(",");
        for (String word : words) {
            List<String> list = new ArrayList();
            list.add(word);
            forward(list);
        }
    }

    // 收尾
    public void close() throws HiveException {

    }
}
