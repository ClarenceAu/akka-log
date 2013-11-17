/**
 * 
 */
package me.clarenceau.log.actor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import me.clarenceau.log.message.Record;
import me.clarenceau.log.message.Result;
import akka.actor.UntypedActor;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class AggregateActor extends UntypedActor {
    
    private Map<String, List<Record>> map  = new ConcurrentHashMap<String, List<Record>>();

    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof Record) {
            Record record = (Record) msg;
            List<Record> list = map.get(record.itemId());
            if (list == null) {
                list = new ArrayList<Record>();
                map.put(record.itemId(), list);
            }
            list.add(record);
        } else if (msg instanceof Result) {
            getSender().tell(Collections.unmodifiableMap(map));
        } else {
            unhandled(msg);
        }
    }

}
