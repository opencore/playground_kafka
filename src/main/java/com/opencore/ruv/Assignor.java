package com.opencore.ruv;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;

public class Assignor {

  public static void main( String[] args ) {
    PartitionAssignor assignor = new RangeAssignor();
    Map<String, PartitionAssignor.Assignment> assignments = new HashMap<>();


    MapUtils.debugPrint(System.out, "Assignments", assignments);
  }
}
