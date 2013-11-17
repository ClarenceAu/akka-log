/**
 * 
 */
package me.clarenceau.log.message;


/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class LogMsg {

    private String line;

    private String[] itemIds;

    public LogMsg(String line, String[] ids) {
        this.line = line;
        itemIds = ids;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public String[] getItemIds() {
        return itemIds;
    }

    public void setItemIds(String[] itemIds) {
        this.itemIds = itemIds;
    }

}
