package nl.vu.cs.ajira.examples.aurora.eval.esper;
// Commented out because we don't include Esper.
//
//package nl.vu.cs.ajira.examples.aurora.eval.esper;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Map;
//import java.util.Random;
//import java.util.Set;
//
//import com.espertech.esper.client.Configuration;
//import com.espertech.esper.client.EPServiceProvider;
//import com.espertech.esper.client.EPServiceProviderManager;
//import com.espertech.esper.client.EPStatement;
//import com.espertech.esper.client.EventBean;
//import com.espertech.esper.client.UpdateListener;
//
//public class EsperEval {
//
//  /**
//   * What to execute
//   */
//  private static final boolean runFiler = false;
//  private static final boolean runMap = false;
//  private static final boolean runAggregate = false;
//  private static final boolean runOrder = true;
//  private static final boolean runMapMultiFilters = false;
//  private static final boolean runJoinEvents = false;
//  private static final boolean runFilterMultiRules = false;
//
//  /**
//   * Constants
//   */
//  private static final String filePath = "Results/";
//  private static final int numMessages = 100000;
//  private static final int maxNumRules = 100;
//
//  /**
//   * Variables
//   */
//  private EPServiceProvider provider;
//  private Set<MyListener> listeners;
//  private Random random;
//  private final Collection<EsperEvent> events;
//
//  public static void main(String args[]) {
//    EsperEval eval = new EsperEval();
//    eval.createProvider();
//    try {
//      eval.runTests();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  private EsperEval() {
//    events = new ArrayList<EsperEvent>();
//  }
//
//  private void runTests() throws Exception {
//    if (runFiler) {
//      for (int seed = 0; seed <= 10; seed++) {
//        random = new Random(seed);
//        initFilter();
//        start("EsperFilter", seed, 1, true);
//      }
//    }
//    if (runMap) {
//      for (int seed = 0; seed <= 10; seed++) {
//        random = new Random(seed);
//        initMap();
//        start("EsperMap", seed, 1, true);
//      }
//    }
//    if (runAggregate) {
//      for (int seed = 0; seed <= 10; seed++) {
//        random = new Random(seed);
//        initAggregate();
//        start("EsperAggregate", seed, 1, true);
//      }
//    }
//    if (runOrder) {
//      for (int seed = 0; seed <= 10; seed++) {
//        random = new Random(seed);
//        initOrder();
//        start("EsperOrder", seed, 1, true);
//      }
//    }
//    if (runMapMultiFilters) {
//      for (int seed = 0; seed <= 10; seed++) {
//        random = new Random(seed);
//        initMapMultiFilters();
//        start("EsperMapMultiFilters", seed, 1, true);
//      }
//    }
//    if (runJoinEvents) {
//      int maxSize = 100;
//      for (int seed = 0; seed <= 10; seed++) {
//        for (int size = 5; size <= maxSize; size += 5) {
//          random = new Random(seed);
//          initJoin(size);
//          start("EsperJoin", seed, 2, size == maxSize);
//        }
//      }
//    }
//    if (runFilterMultiRules) {
//      int numRules = 100;
//      int numEventTypes = 10;
//      for (int seed = 0; seed <= 10; seed++) {
//        random = new Random(seed);
//        initFilterMultiRules(numRules, numEventTypes);
//        start("FilterMultiRules", seed, numEventTypes, true);
//      }
//    }
//  }
//
//  private void createProvider() {
//    Configuration config = new Configuration();
//    config.getEngineDefaults().getLogging().setEnableExecutionDebug(false);
//    config.setMetricsReportingDisabled();
//    Map<String, Object> attributesMap = new HashMap<String, Object>();
//    attributesMap.put("A", "int");
//    attributesMap.put("B", "int");
//    attributesMap.put("C", "int");
//    attributesMap.put("D", "int");
//    for (int i = 0; i < maxNumRules; i++) {
//      config.addEventType("Event_" + i, attributesMap);
//    }
//    config.getEngineDefaults().getThreading().setThreadPoolInbound(false);
//    config.getEngineDefaults().getThreading().setThreadPoolOutbound(false);
//    provider = EPServiceProviderManager.getDefaultProvider(config);
//  }
//
//  private void init(String... queries) {
//    provider.initialize();
//    listeners = new HashSet<MyListener>();
//    for (String query : queries) {
//      EPStatement statement = provider.getEPAdministrator().createEPL(query);
//      MyListener listener = new MyListener();
//      statement.addListener(listener);
//      listeners.add(listener);
//    }
//  }
//
//  private void initFilter() {
//    init("select * from Event_0 where A > 50");
//  }
//
//  private void initMap() {
//    init("select A, B, C from Event_0");
//  }
//
//  private void initAggregate() {
//    init("select AVG(A), B, C from Event_0.win:length(100)");
//  }
//
//  private void initOrder() {
//    //init("select * from Event_0.ext:sort(100, A asc)");
//    init("select * from Event_0.win:length(100) order by A");
//  }
//
//  private void initMapMultiFilters() {
//    init("select A, B, C from Event_0 where A > 50 and B < 100 and C > 0");
//  }
//
//  private void initJoin(int size) {
//    init("select * from Event_0.win:length(" + size + "), Event_1.win:length(" + size + ") where Event_0.A = Event_1.A");
//  }
//
//  private void initFilterMultiRules(int numRules, int numEventTypes) {
//    String[] queries = new String[numRules];
//    for (int i = 0; i < numRules; i++) {
//      String query = "select * from Event_" + i % numEventTypes + " where A > 50";
//      queries[i] = query;
//    }
//    init(queries);
//  }
//
//  private void start(String name, int seed, int numEventTypes, boolean newLine) throws Exception {
//    createEvents(numEventTypes);
//    long startTime = System.nanoTime();
//    for (EsperEvent ev : events) {
//      provider.getEPRuntime().sendEvent(ev.attributes, ev.type);
//    }
//    double totalLatency = ((System.nanoTime() - startTime)) / 1000000.0;
//    System.out.println("Seed: " + seed + " - Overall Processing Time: " + totalLatency + " ms");
//    if (seed > 0) writeToFile(name, seed, totalLatency, newLine);
//  }
//
//  private void createEvents(int numEventTypes) {
//    events.clear();
//    for (int i = 0; i < numMessages; i++) {
//      for (int t = 0; t < numEventTypes; t++) {
//        Map<String, Object> attributesMap = new HashMap<String, Object>();
//        attributesMap.put("A", random.nextInt(100));
//        attributesMap.put("B", random.nextInt(100));
//        attributesMap.put("C", random.nextInt(100));
//        attributesMap.put("D", random.nextInt(100));
//        EsperEvent event = new EsperEvent("Event_" + t, attributesMap);
//        events.add(event);
//      }
//    }
//  }
//
//  private final void writeToFile(String fileName, double label, double value, boolean newLine) {
//    try {
//      FileOutputStream os = new FileOutputStream(new File(filePath + fileName + ".txt"), true);
//      String line = String.valueOf(value);
//      if (newLine) {
//        line = line + "\n";
//      } else {
//        line = line + "\t";
//      }
//      os.write(line.getBytes());
//      os.close();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
//
//  class MyListener implements UpdateListener {
//    private int receivedEvents;
//
//    public MyListener() {
//      receivedEvents = 0;
//    }
//
//    @Override
//    public void update(EventBean[] newData, EventBean[] oldData) {
//      receivedEvents += newData.length;
//    }
//
//    public int getReceivedEvents() {
//      return receivedEvents;
//    }
//  }
// }
