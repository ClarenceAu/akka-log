/**
 * 
 */
package me.clarenceau.log;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import me.clarenceau.log.actor.MasterActor;
import me.clarenceau.log.message.TaskMsg;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class LogAnalysisApplication {
    
    public static long start;

    public static void main(String[] args) throws Exception {
        String dirPath1 = "D:\\tmall\\dacu\\web";
        String dirPath2 = "D:\\tmall\\dacu\\top1";
        String dirPath3 = "D:\\tmall\\dacu\\top2";
        String idFilePath = "D:\\tmall\\dacu\\itemIds.txt";
        List<String> itemIds = readItemIdFromFile(idFilePath);
        List<File> allLogFiles1 = listAllLog(dirPath1);
        List<File> allLogFiles2 = listAllLog(dirPath2);
        List<File> allLogFiles3 = listAllLog(dirPath3);
        System.out.println("Start finding item record: " + itemIds.toString());
        
        ActorSystem _system = ActorSystem.create("LogAnalysisApplication");
        ActorRef master = _system.actorOf(new Props(MasterActor.class),
                "master");
        start = System.nanoTime();
        master.tell(new TaskMsg(allLogFiles1.toArray(new File[allLogFiles1.size()]), 
                                itemIds.toArray(new String[itemIds.size()])));
        master.tell(new TaskMsg(allLogFiles2.toArray(new File[allLogFiles2.size()]), 
                itemIds.toArray(new String[itemIds.size()])));
        master.tell(new TaskMsg(allLogFiles3.toArray(new File[allLogFiles3.size()]), 
                itemIds.toArray(new String[itemIds.size()])));
    }

    private static List<String> readItemIdFromFile(String filePath) {
        BufferedReader br = null;
        List<String> itemIds = new ArrayList<String>();
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(
                    filePath)));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (!"".equals(line)) {
                    itemIds.add(line);
                }
            }
            return itemIds;
        } catch (Exception e) {
            System.err.println("读取商品Id失败");
            throw new RuntimeException(e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<File> listAllLog(String dir) {
        List<File> list = new ArrayList<File>();
        File logDir = new File(dir);
        listFile(logDir, list);
        return list;
    }

    private static void listFile(File file, List<File> list) {
        if (file.isDirectory()) {
            File[] listFiles = file.listFiles(new FileFilter() {
                public boolean accept(File file) {
                    return file.isDirectory()
                            || file.getName().indexOf("dacu.log") != -1;
                }
            });
            for (File f : listFiles) {
                listFile(f, list);
            }
        } else {
            list.add(file);
        }
    }

}
