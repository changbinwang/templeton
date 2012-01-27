#!/usr/local/bin/perl -w
use strict;

my $TESTDIR = '/tmp/templetontest/';
my $TEST_INP_DIR = '/tmp/test_inpdir/'; #dir on hadoop
my $TEST_USER = 'hortonth';
my $WEBHDFS_URL = 'http://localhost:50070';
my $TEMPLETON_URL = 'http://localhost:8080';
my $TOMCAT_HOME = $ENV{'TOMCAT_HOME'};

#use env variables if they have been set
if(defined  $ENV{'TESTDIR'}){
    $TESTDIR = $ENV{'TESTDIR'};
}
if(defined  $ENV{'TEST_INP_DIR'}){
    $TEST_INP_DIR = $ENV{'TEST_INP_DIR'};
}
if(defined  $ENV{'TEST_USER'}){
    $TEST_USER = $ENV{'TEST_USER'};
}
if(defined  $ENV{'WEBHDFS_URL'}){
    $WEBHDFS_URL = $ENV{'WEBHDFS_URL'};
}
if(defined  $ENV{'TEMPLETON_URL'}){
    $TEMPLETON_URL = $ENV{'TEMPLETON_URL'};
}

if(! defined $ENV{'HCAT_PREFIX'}){
    $ENV{'HCAT_PREFIX'}='/usr/';
}

if(! defined $ENV{'HADOOP_PREFIX'}){
    $ENV{'HADOOP_PREFIX'}='/usr/';
}

print STDERR "##################################################################\n";
print STDERR "Using the following settings for environment variables\n" .
    " (Set them to override the default values) \n" .
    "WEBHDFS_URL : $WEBHDFS_URL \n" .
    "TEMPLETON_URL : $TEMPLETON_URL \n" .
    'TOMCAT_HOME :' . $ENV{'TOMCAT_HOME'} . "\n" .
    'HADOOP_PREFIX :' . $ENV{'HADOOP_PREFIX'} . "\n" .
    'HCAT_PREFIX :' . $ENV{'HCAT_PREFIX'} . "\n" 
;
print STDERR "##################################################################\n";

system("rm -rf $TESTDIR/");

#restart tomcat with updated env variables
my $templeton_src = "$TESTDIR/templeton_src";
$ENV{'TEMPLETON_HOME'} = "$templeton_src/templeton";

system ("$TOMCAT_HOME/bin/shutdown.sh") == 0 or die "tomcat shutdown failed" ;
sleep 3;



#get templeton git repo, build and install
system("mkdir -p $templeton_src") == 0 or die "could not create dir $templeton_src: $!";
chdir  "$templeton_src"  or die "could not change directory to  $templeton_src  : $!";
system ('git clone  git@github.com:hortonworks/templeton.git')  == 0 or die "could not clone templeton git repo";
chdir 'templeton' or die 'could not change dir : $!';


#put a templeton-site.xml in $TEMPLETON_HOME with zookeeper hostname
writeTempletonSiteXml();
system ('ant install-war') == 0 or die "templeton build failed";

#tomcat should have shutdown by now, try starting it
system ("$TOMCAT_HOME/bin/startup.sh") == 0 or die 'tomcat startup failed';
sleep 3;

my $tdir = "$templeton_src/templeton/src/test/e2e/templeton";
chdir $tdir or die "could not change dir $tdir : $!";

#copy input files
system("hadoop fs -rmr $TEST_INP_DIR");
system("hadoop fs -copyFromLocal $tdir/inpdir $TEST_INP_DIR") == 0 or die "failed to copy input dir : $!";
system("hadoop fs -chmod -R 777 $TEST_INP_DIR")  == 0 or die "failed to set input dir permissions : $!";

#start tests
system ("ant test -Dinpdir.hdfs=$TEST_INP_DIR  -Dtest.user.name=$TEST_USER -Dharness.webhdfs.url=$WEBHDFS_URL -Dharness.templeton.url=$TEMPLETON_URL") == 0 or die "templeton tests failed";


#############################
sub writeTempletonSiteXml {
    my $conf = $ENV{'TEMPLETON_HOME'} . "/templeton-site.xml";
    open ( CFH,  ">$conf" ) or die $!;
    my $host = `hostname` ;
    chomp $host;
    my $zookeeper_host = $host . ':2181';
    if( defined $ENV{'ZOOKEEPER_HOST'}){
	$zookeeper_host = $ENV{'ZOOKEEPER_HOST'}
    }

    print CFH '<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>templeton.zookeeper.hosts</name>
    <value>'  . 
 $zookeeper_host .
'</value>
    <description>ZooKeeper servers, as comma separated host:port pairs</description>
  </property>

  <property>
    <name>templeton.hive.properties</name>
    <value>hive.metastore.local=false,hive.metastore.uris=thrift://' .
    $host .
    ':9933,hive.metastore.sasl.enabled=false,hive.metastore.execute.setugi=true</value>
    <description>Properties to set when running hive.</description>
  </property>
</configuration>
';
    close CFH or die $!;

;





}
