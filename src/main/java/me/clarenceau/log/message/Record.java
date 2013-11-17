/**
 * 
 */
package me.clarenceau.log.message;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class Record {

    Date time;
    String userId;
    String itemId;
    String skuId;
    String quantity;

    String method;
    String type;
    String result;

    public static Record newRecord() {
        return new Record();
    }

    public Record time(String time) throws Exception {
        this.time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").parse(time);
        return this;
    }
    
    public Date time() {
        return time;
    }

    public Record userId(String userId) {
        this.userId = userId;
        return this;
    }

    public Record itemId(String itemId) {
        this.itemId = itemId;
        return this;
    }
    
    public String itemId() {
        return itemId;
    }

    public Record skuId(String skuId) {
        if ("null".equals(skuId)) {
            skuId = "0";
        } else {
            this.skuId = skuId;
        }
        return this;
    }

    public Record quantity(String quantity) {
        this.quantity = quantity;
        return this;
    }

    public Record method(String method) {
        this.method = method;
        return this;
    }

    public Record result(String result) {
        this.result = result;
        return this;
    }

    public Record type(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String toString() {
        return "Record [time=" + time + ", userId=" + userId + ", itemId="
                + itemId + ", skuId=" + skuId + ", quantity=" + quantity
                + ", method=" + method + ", type=" + type + ", result="
                + result + "]";
    }
}
