/**
 * 
 */
package me.clarenceau.log.actor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import me.clarenceau.log.LogAnalysisApplication;
import me.clarenceau.log.message.LogMsg;
import me.clarenceau.log.message.ReadFinished;
import me.clarenceau.log.message.Record;
import me.clarenceau.log.message.Result;
import me.clarenceau.log.message.TaskMsg;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.RoundRobinRouter;

/**
 * @author <a href="zhencong.ouzc@taobao.com">遣怀</a>
 * @date 2013年11月17日
 */
public class MasterActor extends UntypedActor {
    
    private Set<String> itemIds = new HashSet<String>();
    
    private AtomicInteger counter = new AtomicInteger(0);
    
    private AtomicInteger sum = new AtomicInteger(0);
    
    ActorRef matcherActor = getContext().actorOf(
            new Props(MatcherActor.class).withRouter(new RoundRobinRouter(100)),
            "matcher");
    ActorRef aggregateActor = getContext().actorOf(
            new Props(AggregateActor.class),
            "aggregator");
    ActorRef readerActor = getContext().actorOf(new Props(ReadFileActor.class).withRouter(new RoundRobinRouter(3)), "reader");


    @SuppressWarnings("unchecked")
    @Override
    public void onReceive(Object msg) throws Exception {
        if (msg instanceof TaskMsg) {
            
            TaskMsg task = (TaskMsg) msg;
            Collections.addAll(itemIds, task.getItemIds());
            File[] files = task.getInputs();
            int total = sum.addAndGet(files.length);
            System.out.println("Total files: " + total);
            readerActor.tell(files, getSelf());
        } else if (msg instanceof String) {
            matcherActor.tell(new LogMsg((String)msg, itemIds.toArray(new String[itemIds.size()])), getSelf());
        } else if (msg instanceof Record) {
            aggregateActor.tell(msg, getSelf());
        } else if (msg instanceof ReadFinished) {
            int count = counter.incrementAndGet();
            int total = sum.get();
            if (count == total) {
                aggregateActor.tell(new Result(), getSelf());
            }
        } else if (msg instanceof Map) {
            Map<String, List<Record>> map = (Map<String, List<Record>>) msg;
            for (Map.Entry<String, List<Record>> entry : map.entrySet()) {
                writeItemQuantityToFile(entry.getKey(), entry.getValue(), "D:\\tmall\\dacu");
            }
            getContext().system().shutdown();
            System.out.println(TimeUnit.MILLISECONDS.convert((System.nanoTime() - LogAnalysisApplication.start), TimeUnit.NANOSECONDS));
        } else {
            unhandled(msg);
        }

    }
    
    private static void writeItemQuantityToFile(String itemId, List<Record> quantityRecordList, String outputDir) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDir + File.separator + itemId + ".csv")));
            bw.append("time,itemId,skuId,quantity,seller,method,type,result\n");
            for (Record itemQuantityRecord : quantityRecordList) {
                bw.append(itemQuantityRecord.toString());
                bw.newLine();
            }
        } catch (Exception e) {
            System.err.println("写入结果文件:" + (outputDir + itemId + ".csv") + "出错");
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    static class RecordComparator implements Comparator<Record> {

        @Override
        public int compare(Record o1, Record o2) {
            return o1.time().compareTo(o2.time());
        }
        
    }

}
