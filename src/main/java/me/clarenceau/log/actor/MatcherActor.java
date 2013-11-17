/**
 * 
 */
package me.clarenceau.log.actor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import me.clarenceau.log.message.LogMsg;
import me.clarenceau.log.message.Record;
import akka.actor.UntypedActor;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class MatcherActor extends UntypedActor {
    
    private static Pattern pWithoutType = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) WARN  - DaCu : Quantity: \\[([\\w\\.]+)\\] \\[(true|false)\\] (sellerId|sellerNick) = (.+), itemId = (\\d+), skuId = (\\d+), quantity = (\\d+)");
    private static Pattern pWithoutSku = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) WARN  - DaCu : Quantity: \\[([\\w\\.]+)\\] \\[(true|false)\\] (sellerId|sellerNick) = (.+), itemId = (\\d+), quantity = (\\d+)");
    private static Pattern pWithType = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) WARN  - DaCu : Quantity: \\[([\\w\\.]+)\\] \\[(true|false)\\] (sellerId|sellerNick) = (.+), itemId = (\\d+), skuId = (null|\\d+), type = (\\d+), quantity = (\\d+)");
    
    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof LogMsg) {
            LogMsg logMsg = (LogMsg) msg;
            String[] itemIds = logMsg.getItemIds();
            String line = logMsg.getLine();
            Record record = matchAndBuildRecord(line, itemIds);
            if (record != null) {
                getSender().tell(record);
            }
        } else {
            unhandled(msg);
        }
    }

    private Record matchAndBuildRecord(String line, String[] itemIds) {
        boolean flag = false;
        for (String itemId : itemIds) {
            if (line.indexOf("itemId = " + itemId, 100) != -1) {
                flag = true;
                break ;
            }
        }
        
        if (!flag) {
            return null;
        }
        
        Matcher m = null;
        if (line.indexOf("type") != -1) {
            m = pWithType.matcher(line);
        } else {
            if (line.indexOf("skuId") != -1) {
                m = pWithoutType.matcher(line);
            } else {
                m = pWithoutSku.matcher(line);
            }
        }
        
        if (m.find()) {
            try {
                Record record = Record.newRecord()
                                      .time(m.group(1))
                                      .method(m.group(2))
                                      .result(m.group(3))
                                      .userId(m.group(5))
                                      .itemId(m.group(6));
                if (line.indexOf("skuId") != -1) {
                    record.skuId(m.group(7));
                }
                if (line.indexOf("type") != -1) {
                    record.type(m.group(8))
                          .quantity(m.group(9));
                } else {
                    if (line.indexOf("skuId") != -1) {
                        record.quantity(m.group(8));
                    } else {
                        record.quantity(m.group(7));
                    }
                }
                return record;
            } catch (Exception e) {
                System.err.println(e.toString() + ", " + line);
            }
        }
        return null;
    }
    
}
