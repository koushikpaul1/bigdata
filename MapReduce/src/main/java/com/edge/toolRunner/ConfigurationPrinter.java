package com.edge.toolRunner;

/*These are properties which  are already set
 *To set a new one, or to override one 
 *in runtime using -D option in 
 * Run as>Run config>Progam arg 
 * -D color=yellow
 * -D io.sort.mb=999
 * -D mapreduce.job.reduces=7
-D property=value 				-> Sets the given Hadoop configuration property to the given value. Overrides any default or site properties in the configuration and any properties set via the -conf option.
-conf filename ...				->  Adds the given files to the list of resources in the configuration. This is a convenient way to set site properties or to set a number of properties at once.
-fs uri 						-> Sets the default filesystem to the given URI. Shortcut for -D fs.defaultFS=uri.
-jt host:port					-> Sets the YARN resource manager to the given host and port. (In Hadoop 1, it sets the jobtracker address, hence the option name.) Shortcut for -D yarn.resourcemanager.address=host:port.
-files file1,file2,... 			-> Copies the specified files from the local filesystem (or any filesystem if a scheme is specified) to the shared filesystem used by MapReduce (usually HDFS) and makes them available to MapReduce programs in the task’s working directory. (See Distributed Cache for more on the distributed cache mechanism for copying files to machines in the cluster.)
-archives archive1,archive2,...	-> Copies the specified archives from the local filesystem (or any filesystem if a scheme is specified)to the shared filesystem used by MapReduce (usually HDFS), unarchives them, and makes them available to MapReduce programs in the task’s working directory.
-libjars jar1,jar2,... 			-> Copies the specified JAR files from the local filesystem (or any filesystem if a scheme is specified)to the shared filesystem used by MapReduce (usually HDFS) and adds them to the MapReduce task’s classpath. This option is a useful way of shipping JAR files that a job is dependent on.
 * */


import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {
static {
Configuration.addDefaultResource("hdfs-default.xml");
Configuration.addDefaultResource("hdfs-site.xml");
Configuration.addDefaultResource("yarn-default.xml");
Configuration.addDefaultResource("yarn-site.xml");
Configuration.addDefaultResource("mapred-default.xml");
Configuration.addDefaultResource("mapred-site.xml");
}
public int run(String[] args) throws Exception {
Configuration conf = getConf();
int i=1;
for (Entry<String, String> entry: conf) {
System.out.printf(++i +"  %s=%s\n", entry.getKey(), entry.getValue());
}
return 0;
}
public static void main(String[] args) throws Exception {
int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
System.exit(exitCode);
}
}