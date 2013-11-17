/**
 * 
 */
package me.clarenceau.log.message;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class MatchedMsg {

    private Map<String, List<String>> map;
    
    public MatchedMsg() {
        map = new HashMap<String, List<String>>();
    }
    
    public void addMatchedResult(String itemId, String line) {
        List<String> list = map.get(itemId);
        if (list == null) {
            list = new LinkedList<String>();
            map.put(itemId, list);
        }
        list.add(line);
    }
    
    public Map<String, List<String>> getMatchedResult() {
        return Collections.unmodifiableMap(map);
    }
}
