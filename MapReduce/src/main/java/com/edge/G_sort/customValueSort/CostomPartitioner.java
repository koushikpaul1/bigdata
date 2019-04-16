/**
 * The only reason for this class is not to use the whole key(Employee), but only deptno.
 * On a closer look it was found that using or not using this class doesn't make any difference
 */
package com.edge.G_sort.customValueSort;

/**
 * @author New
 *
 */

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class CostomPartitioner extends Partitioner<Employee, Text> {	
	public int getPartition(Employee key, Text value, int numReduceTasks) {	
		return Math.abs(key.getDeptNo().hashCode() % numReduceTasks);
	}
}