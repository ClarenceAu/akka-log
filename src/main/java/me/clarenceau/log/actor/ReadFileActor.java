/**
 * 
 */
package me.clarenceau.log.actor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import me.clarenceau.log.message.ReadFinished;
import akka.actor.UntypedActor;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class ReadFileActor extends UntypedActor {
    
    private static AtomicInteger counter = new AtomicInteger(0);
    
    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof File[]) {
            File[] files = (File[]) msg;
            for (File file : files) {
                BufferedReader br = null;
                try {
                    br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    String line = null;
                    while ( (line = br.readLine()) != null ) {
                        getSender().tell(line);
                    }
                } catch (Exception e) {
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
                getSender().tell(new ReadFinished());
            }
        } else {
            unhandled(msg);
        }
    }
    
    private List<String> readLineFromFile(File file) {
        List<String> lines = new LinkedList<String>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line = null;
            while ( (line = br.readLine()) != null ) {
                lines.add(line);
            }
            return lines;
        } catch (Exception e) {
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
}
