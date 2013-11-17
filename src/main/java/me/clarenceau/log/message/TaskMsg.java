/**
 * 
 */
package me.clarenceau.log.message;

import java.io.File;
import java.util.Arrays;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class TaskMsg {

    private File[] inputs;

    private String[] itemIds;

    public TaskMsg(File[] inputs, String[] itemIds) {
        this.inputs = inputs;
        this.itemIds = itemIds;
    }

    public File[] getInputs() {
        return inputs;
    }

    public void setInputs(File[] inputs) {
        this.inputs = inputs;
    }

    public String[] getItemIds() {
        return itemIds;
    }

    public void setItemIds(String[] itemIds) {
        this.itemIds = itemIds;
    }

    @Override
    public String toString() {
        return "TaskMsg [inputs=" + Arrays.toString(inputs) + ", itemIds="
                + Arrays.toString(itemIds) + "]";
    }

}
