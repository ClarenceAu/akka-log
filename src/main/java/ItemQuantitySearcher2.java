/**
 * 
 */
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @desc data - data.Main.java
 * @author qianhuai
 * @email zhencong.ouzc@taobao.com
 * @date 2013年9月27日
 */
public class ItemQuantitySearcher2 {
	
	private static Pattern pWithoutType = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) WARN  - DaCu : Quantity: \\[([\\w\\.]+)\\] \\[(true|false)\\] (sellerId|sellerNick) = (.+), itemId = (\\d+), skuId = (\\d+), quantity = (\\d+)");
	private static Pattern pWithType = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) WARN  - DaCu : Quantity: \\[([\\w\\.]+)\\] \\[(true|false)\\] (sellerId|sellerNick) = (.+), itemId = (\\d+), skuId = (null|\\d+), type = (\\d+), quantity = (\\d+)");
	
	private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors() * 2;
	
		
	public static void main(String[] args) throws Exception {
			
		String dirPath = "d:\\dacu\\dacu.log.35853890793";
		String outputDirPath = "d:\\dacu\\";
		String idFilePath = "d:\\dacu\\itemIds.txt";
		
		List<String> itemIds = readItemIdFromFile(idFilePath);
		
		findItemQuantityRecord(itemIds, dirPath, outputDirPath);
	}
		
	private static List<String> readItemIdFromFile(String filePath) {
		BufferedReader br = null;
		List<String> itemIds = new ArrayList<String>();
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
			String line = null;
			while ( (line = br.readLine()) != null ) {
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
	
	public static void findItemQuantityRecord(final List<String> itemIds, String logDir, String outputDir) {
		System.out.println("商品库存更新记录读取开始");
		long start = System.nanoTime();
		// 初始化存放结果的map
		final Map<String, Queue<ItemQuantityRecord>> recordMap = new ConcurrentHashMap<String, Queue<ItemQuantityRecord>>(itemIds.size());
		for (String itemId : itemIds) {
			Queue<ItemQuantityRecord> quantityRecordQueue = new ConcurrentLinkedQueue<ItemQuantityRecord>();
			recordMap.put(itemId, quantityRecordQueue);
		}
		// 递归读取给定的文件夹下的所有log文件
		final List<File> allLogFiles = listAllLog(logDir);
		// 初始化线程池 和 并发控制相关的变量
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
		final CountDownLatch latch = new CountDownLatch(allLogFiles.size());
		
		for (final File logFile : allLogFiles) {
			executor.submit(new Runnable() {
				@Override
				public void run() {
					try {
						readUserRecordFormFile(logFile, itemIds, recordMap);
					} catch (Exception e) {
					} finally {
						latch.countDown();
					}
				}
			});
		}
		try {
			latch.await();
			executor.shutdown();
		} catch (InterruptedException e) {
		}
		// 排序 输出 
		for (Map.Entry<String, Queue<ItemQuantityRecord>> entry : recordMap.entrySet()) {
			List<ItemQuantityRecord> list = sort(entry.getValue());
			writeItemQuantityToFile(entry.getKey(), list, outputDir);
		}
		
		long duration = TimeUnit.SECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		System.out.println("商品库存更新记录读取结束, 用时:" + duration + " sec");
	}

	private static List<ItemQuantityRecord> sort(Queue<ItemQuantityRecord> queue) {
		ItemQuantityRecord[] array = queue.toArray(new ItemQuantityRecord[queue.size()]);
		List<ItemQuantityRecord> list = new ArrayList<ItemQuantityRecord>(array.length);
		Collections.addAll(list, array);
		Collections.sort(list, new RecordComparator());
		return list;
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
				@Override
				public boolean accept(File file) {
					return file.isDirectory() || file.getName().indexOf("dacu.log") != -1;
				}
			});
			for (File f : listFiles) {
				listFile(f, list);
			}
		} else {
			list.add(file);
		}
	}
	
	private static void readUserRecordFormFile(File logFile, List<String> targetItemIds, Map<String, Queue<ItemQuantityRecord>> recordMap) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(logFile));
			String line = null;
			while ( (line = br.readLine()) != null ) {
				boolean flag = false;
				for (String itemId : targetItemIds) {
					if (line.indexOf("itemId = " + itemId, 100) != -1) {
						flag = true;
						break ;
					}
				}
				
				if (!flag) {
					continue ;
				}
				
				Matcher m = null;
				if (line.indexOf("type") != -1) {
					m = pWithType.matcher(line);
				} else {
					m = pWithoutType.matcher(line);
				}
				
				if (m.find()) {
					try {
						ItemQuantityRecord record = ItemQuantityRecord
								                  .newItemQuantityRecord()
							                      .time(m.group(1))
							                      .method(m.group(2))
							                      .result(m.group(3))
							                      .userId(m.group(5))
							                      .itemId(m.group(6))
							                      .skuId(m.group(7));
						if (line.indexOf("type") != -1) {
							record.type(m.group(8))
								  .quantity(m.group(9));
						} else {
							record.quantity(m.group(8));
						}
						recordMap.get(record.itemId).add(record);
					} catch (Exception e) {
						System.err.println(e.toString() + ", " + line);
					}
				}
			}
		} catch (Exception e) {
			System.err.println("读取日志文件:" + logFile.getName() + "出错");
			e.printStackTrace();
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
	
	private static void writeItemQuantityToFile(String itemId, List<ItemQuantityRecord> quantityRecordList, String outputDir) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputDir + File.separator + itemId + ".csv")));
			bw.append("time,itemId,skuId,quantity,seller,method,type,result\n");
			for (ItemQuantityRecord itemQuantityRecord : quantityRecordList) {
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
	
	static class RecordComparator implements Comparator<ItemQuantityRecord> {

		@Override
		public int compare(ItemQuantityRecord o1, ItemQuantityRecord o2) {
			return o1.time.compareTo(o2.time);
		}
		
	}

	static class ItemQuantityRecord {
		
		
		Date time;
		String userId;
		String itemId;
		String skuId;
		String quantity;
		
		String method;
		String type;
		String result;
		
		static ItemQuantityRecord newItemQuantityRecord() {
			return new ItemQuantityRecord();
		}
		
		ItemQuantityRecord time(String time) throws Exception {
			this.time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").parse(time);
			return this;
		}
		ItemQuantityRecord userId(String userId) {
			this.userId = userId;
			return this;
		}
		ItemQuantityRecord itemId(String itemId) {
			this.itemId = itemId;
			return this;
		}
		ItemQuantityRecord skuId(String skuId) {
			if ("null".equals(skuId)) {
				skuId = "0";
			} else {
				this.skuId = skuId;
			}
			return this;
		}
		ItemQuantityRecord quantity(String quantity) {
			this.quantity = quantity;
			return this;
		}
		ItemQuantityRecord method(String method) {
			this.method = method;
			return this;
		}
		ItemQuantityRecord result(String result) {
			this.result = result;
			return this;
		}
		ItemQuantityRecord type(String type) {
			this.type = type;
			return this;
		}
		public String toString() {
			return String.format("%s,%s,%s,%s,%s,%s,%s,%s", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(time), itemId, skuId, quantity, userId, method, type, result);
		}
	}

}
