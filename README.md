# Spark performance test harness

## Goal of this test harness

* This program's aim is to measure spark performance on a number of operations and and typical use cases.
  * Parsing of JSONL Dataframe file. JSONL Dataframe file is a format designed to be human readable text while somewhat
  optimized for large data transfers over the network. It essentially includes exactly ONE meta data line that defines
  data types column names for the consecutive lines that contain compact arrays of data. Current JSONL Dataframe assumes 
  support for the following types:
    * String, LocalDate, Timestamp, BigDecimal, BigInteger, Array<BigDecimal>, Array<BigInteger>
  * Supporting scripts create a number of JSONL Dataframe files of sizes that vary in number of rows as well as number 
  of columns with enough scenarios to give you a sense for spark performance of both parsing this data and converting 
  it to a spark Dataframe as well as performing operations such as count and save to hdfs 
    
# Build/Run Environment setup
## Tested on Mac OS
  * Instructions below will using [Homebrew](https://brew.sh/) if you don't already have it install it first.
   
    ![alt_text](README.md.resources/macOS-version.png)

***
# HDFS Install & Starting Service

## Install HDFS & Yarn
  * `brew install hadoop`
    * This will install hadoop that includes __HDFS__ and __YARN__ service under `/usr/local/Cellar/hadoop/3.1.2` which will
    references as `${HADOOP_HOME}` further in instructions.
  * Change property `hadoop.tmp.dir` to the following value `~/hadoop-storage`, in this file 
    `${HADOOP_HOME}/libexec/etc/hadoop/core-default.xml`. The default value `/tmp/hadoop-${user.name}` results in a location
    that gets erased after reboot and __HDFS__ gets corrupted after your computer restart.
  * Enable HADOOP Pseudo-Distributed Operations mode. Summary is included below, for more details and options visit
  [hadoop.apache.org](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)    

### Configuration Summary __HDFS__ & __YARN__
  * Locate the file `${HADOOP_HOME}/libexec/etc/hadoop/core-site.xml` and add the following property
    ~~~xml
    <configuration>
        <property>
            <name>fs.defaultFS</name>
            <value>hdfs://localhost:9000</value>
        </property>
    </configuration>
    ~~~
  * Locate file `${HADOOP_HOME}/libexec/etc/hadoop/hdfs-site.xml` and add the following property
    ~~~xml
    <configuration>
        <property>
            <name>dfs.replication</name>
            <value>1</value>
        </property>    
    </configuration>
    ~~~
  * Ensure that `$ ssh localhost` has key based authentication if not follow steps below to enable.
    ~~~shell script
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 0600 ~/.ssh/authorized_keys 
    ~~~
  * Format __HDFS__ file system 
    ~~~shell script
    ${HADOOP_HOME}/libexec/bin/hdfs namenode -format
    ~~~

### Starting __HDFS__ file system, creating home directory for the harness & __YARN__ resource manager
  * Start __HDFS__ & Format the disk
    ~~~shell script
    ${HADOOP_HOME}/libexec/sbin/start-dfs.sh
    ${HADOOP_HOME}/libexec/bin/hdfs dfs -mkdir -p /user/test-harness
    ${HADOOP_HOME}/libexec/sbin/start-yarn.sh
    ~~~
  * Above should start several services for which UI will be visible on the following default ports
    * __HDFS__ - [http://localhost:9870/]
    * __YARN__ - [http://localhost:8088/]
  * To stop __HDFS__ & __YARN__ use the following scripts
    ~~~shell script
    ${HADOOP_HOME}/libexec/sbin/stop-dfs.sh
    ${HADOOP_HOME}/libexec/sbin/stop-yarn.sh
    ~~~
   
## Load HDFS with randomly generated data samples.
  * `brew install maven`
    * Scripts generating data samples and writing them to __HDFS__ are written in python can be executed 
    using a maven goal. Hence we recommend you install maven as well as anaconda and create environment 
    using provided recipe.
  * `brew casks install anaconda` followed by `conda init bash`
    * Restart your shell terminal, after installing anaconda and initializing bash shell to recognize location of conda binaries 
  * `conda env create -f ./utils/conda.recipe/test-harness.yml`
    * you will need to run this command in the directory where you check out this test-harness project from [GitHub](https://github.com/alexshagiev/spark-test-harness)
    This will create a python environment with necessary hdfs libraries to run data generator 
  * `conda activate test-harness`
    * Activate conda environment you had created. Data generator scrip needs to run in this specific __Conda__ environment
  * `mvn exec:exec@generate-data`
    * Run maven goal to populate __HDFS__ with data scenarios defined in [application.conf#scenarios/run section](./src/main/resources/application.conf)
  * [http://localhost:9870/explorer.html#/user/test-harness/data/l0/jsonl](http://localhost:9870/explorer.html#/user/test-harness/data/l0/jsonl)
    * You should be able to see the data being generated using File Browser in the link avove. 

# Spark Install & Starting Stand Alone Service

## Spark Install & Starting the service

### Install Spark
  * `brew install apache-spark`
    * This will install __Apache Spark__ service under `/usr/local/Cellar/apache-spark/2.4.3/` which will refer to as `${SPARK_HOME}` further in instructions.
  * Configure Spark to inherit HDFS configuration
    * Locate file `${SPARK_HOME}/libexec/conf/spak-env.sh` and set the following variables. If the file is missing create one or copy from the template
    ~~~shell script
    HADOOP_HOME=/usr/local/Cellar/hadoop/3.1.2
    HADOOP_CONF_DIR=${HADOOP_HOME}/libexec/etc/hadoop
    ~~~

### Start Stand Alone Spark Cluster
  * Start script
  ~~~shell script
  ${SPARK_HOME}/libexec/sbin/start-all.sh
  ~~~
  * Above should start a service for which UI will be visible on the following default ports
    * __SPARK__ UI - http://localhost:8080/
    * Default Spark master url to which `spark-submit` script will be submitting jobs should located at - `spark://localhost:7077`
  * Stop script
  ~~~shell script
  ${SPARK_HOME}/libexec/sbin/stop-all.sh
  ~~~

# Amazon Web Services Elastic Compute ( EC2 ) & Elastic Map Reduce ( EMR )
## Usefull References on AWS & EMR
  * [Product Info](https://aws.amazon.com/emr/)
  * [Pricing](https://aws.amazon.com/emr/pricing/)
  * [Getting Started and explain of Free Tier](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html)
  * [Free tier allowance](https://console.aws.amazon.com/billing/home#/freetier)
    
## Setting up EMR Spark Cluster
  * [Create a key pair using EC2 console](https://console.aws.amazon.com/ec2/home?#KeyPairs) 
    * Save the PEM key, you will need it later when connecting to the EMR via SSH
    
    ![alt_text](README.md.resources/aws-ec2-key-creating.png)
  * [Use Create Cluster button](https://console.aws.amazon.com/elasticmapreduce/)
    * I turned off logging as it seems to cause excessive AWS S3 storage utilization and cost
    * Remember to select the key you created in the prior step to be able to access your master node from public internet.
    
    ![alt_text](README.md.resources/aws-emr-cluster-settings.png)
  * Make a note of your __Master Public DNS Name__, this is your entry point into EMR cluster from public internet. We will refer 
  to it as `${MASTER_PUBLIC_DNS_NAME}`
  
    ![alt_text](README.md.resources/aws-erm-cluster-waiting.png) 
  * Use key to login to you Master host `ssh -i ~/aws-emr-key.pem hadoop@${MASTER_PUBLIC_DNS_NAME}` 
  * Execute the following script to configure environment for the test harness to run
  ~~~shell script
  # install git
  sudo yum -y install git
  
  # install anaconda
  rm -rf Anaconda2-2019.07-Linux-x86_64.sh
  rm -rf ./anaconda2
  wget https://repo.continuum.io/archive/Anaconda2-2019.07-Linux-x86_64.sh
  bash Anaconda2-2019.07-Linux-x86_64.sh -b -p ./anaconda2
  ./anaconda2/condabin/conda init bash
  source .bashrc
  
  # install apache maven
  sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
  sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
  sudo yum install -y apache-maven
  
  # maven install with jdk 1.7 so fix env to point to jdk 1.8
  sudo yum install -y java-1.8.0-devel
  sudo /usr/sbin/alternatives --set java /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/java
  sudo /usr/sbin/alternatives --set javac /usr/lib/jvm/java-1.8.0-openjdk.x86_64/bin/javac
  
  # downloand test harness
  rm -rf ./spark-test-harness
  git clone https://github.com/alexshagiev/spark-test-harness.git
  cd spark-test-harness
  ~~~
## Load HDFS with randomly generated data samples & Accessing Cluster UI services from your computer

### Setup dynamic port forwarding for HDFS,Yarn,Spark UI access
  * Install [FoxyProxy](https://chrome.google.com/webstore/detail/foxyproxy-standard/gcknhkkoolaabfmlnjonogaaifnjlfnp?hl=en) extension in your Chrome Browser
    * Configure FoxyProxy as follows [Latest Proxy Settings & plugin instructions here here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html)
    ~~~xml
    <?xml version="1.0" encoding="UTF-8"?>
    <foxyproxy>
       <proxies>
          <proxy name="emr-socks-proxy" id="2322596116" notes="" fromSubscription="false" enabled="true" mode="manual" selectedTabIndex="2" lastresort="false" animatedIcons="true" includeInCycle="true" color="#0055E5" proxyDNS="true" noInternalIPs="false" autoconfMode="pac" clearCacheBeforeUse="false" disableCache="false" clearCookiesBeforeUse="false" rejectCookies="false">
             <matches>
                <match enabled="true" name="*ec2*.amazonaws.com*" pattern="*ec2*.amazonaws.com*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*ec2*.compute*" pattern="*ec2*.compute*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="10.*" pattern="http://10.*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*10*.amazonaws.com*" pattern="*10*.amazonaws.com*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" />
                <match enabled="true" name="*10*.compute*" pattern="*10*.compute*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false" /> 
                <match enabled="true" name="*.compute.internal*" pattern="*.compute.internal*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false"/>
                <match enabled="true" name="*.ec2.internal* " pattern="*.ec2.internal*" isRegEx="false" isBlackList="false" isMultiLine="false" caseSensitive="false" fromSubscription="false"/>	  
           </matches>
             <manualconf host="localhost" port="8157" socksversion="5" isSocks="true" username="" password="" domain="" />
          </proxy>
       </proxies>
    </foxyproxy>
    ~~~
  * Start a dynamic port proxy `ssh -i ~/aws-emr-key.pem -ND 8157 hadoop@${MASTER_PUBLIC_DNS_NAME}`
  * The EMR Management console should now have the WebLinks under `Connections:` section enabled which will take you directly to the 
  [HDFS, SPARK, YARN UIs](https://console.aws.amazon.com/elasticmapreduce/)
  
  ![alt_text](README.md.resources/aws-erm-cluster-waiting.png) 

# Scripted Creation of EMR cluster
 * Create a Free Tier `t2.micro` instance, includes 1 CPU, 1G RAM, Transient [Elastic Brock Storage (EBS)](https://aws.amazon.com/ebs/)
 * Configure Your [AWS Command Line Interface (CLI)](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
 * Create a new `emr-create-id` Identity [Identity Accesss Management ]( https://console.aws.amazon.com/iam/home#/users), chose Programmatic Type for access. This id needs to be assigned to a new group `emr-create-group` withe `
AmazonElasticMapReduceFullAccess` role 
 * When finished creating you will be given `Access Key ID` and `Secret Access Key`, save both you will not be able to retrive them later. 
 * Run `aws configure` on your aws EC2 micro node and assign KeyID & AccessKey from the new `emr-create-id`
 Identity for region use `us-east-2` other appropriate defaults. Make sure the output format is `json`
 * `aws emr create-cluster --release-label emr-5.26.0 --use-default-roles --applications Name=Spark Name=Hadoop --ec2-attributes KeyName=aws-emr-key --instance-fleets InstanceFleetType=MASTER,TargetSpotCapacity=1,InstanceTypeConfigs=['{InstanceType=m4.large}'] InstanceFleetType=CORE,TargetSpotCapacity=2,InstanceTypeConfigs=['{InstanceType=m4.large}'] --auto-terminate`
 * TODO cluster needs to be created with a security group that allows connection from the t2.micro by adding port 8020 to the inbound rules of the `ElasticMapReduce-master` security group
 * ssh -i ~/aws-emr-key.pem hadoop@ec2-18-223-106-115.us-east-2.compute.amazonaws.com hdfs dfs -mkdir -p /user/test-harness - create test harness dir
 * grant rwx permissions to all files ssh -i ~/aws-emr-key.pem hadoop@ec2-18-223-106-115.us-east-2.compute.amazonaws.com hdfs dfs -chmod -R 777 /user/test-harness
 ****** mvn -DskipTests package exec:exec@run-test-aws-emr
 
# Load HDFS
  * `conda env create -f ./utils/conda.recipe/test-harness.yml`
  * you will need to run this command in the directory where you check out this test-harness project from [github.com]
    This will create a python environment with necessary hdfs libraries to run data generator 
  * `conda activate test-harness`
    * Activate conda environment you had created. Data generator scrip needs to run in this specific __Conda__ environment
  * `mvn exec:exec@generate-data`
    * Run maven goal to populate __HDFS__ with data scenarios defined in [application.conf#scenarios/run section](./src/main/resources/application.conf)
  * [http://localhost:9870/explorer.html#/user/test-harness/data/l0/jsonl](http://localhost:9870/explorer.html#/user/test-harness/data/l0/jsonl)
    * You should be able to see the data being generated using File Browser in the link avove. 

  
4. Remember to adjust the following two files to match EMR settings
  * [pom.xml properites/hadoop.config.dir section](pom.xml)
  * [application.conf conf/hdfs/url section](src/main/resources/application.conf) - correct URL:PORT is available on the 
  HDFS Main web interface which can be found [here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-web-interfaces.html)  

# Running test harness
## Submitting spark job using pre-defined spark url
1. `mvn -DskipTests package exec:exec@run-test-spark-master`
   * This command will create an UBER jar and submit it into the stand alone cluster, spark url as defined in [pom.xml's respective maven goal](pom.xml)
   * You can change this option `spark.cores.max` to see how well the scenario scales with less or more cores. 
   When missing a max `1024` cores will be used if available. Typically running this harness on a single host will result in 4 cores
## Submitting spark job using Yarn
1. `mvn -DskipTests package exec:exec@run-test-yarn-master`

## Submitting spark job using InteliJ Run/Debug mode
1. Make the following class to be your main `com.alex.shagiev.spark.Main`
1. Enable `Include dependencies with "Provided" Scope` option in the Run/Debug configuration.
3. Add environment variable `master=local`, this will force the job submit to run in process


# Performance results
## Single Host deployment without yarn
* It appears that performance increases with number of cores but does not exactly double when doubling cores
* Another observation is that it appears that most of the processing cost is JSON parsing, need to look at optimizing further
Example: A file with 1m rows can be counted with out parsing in under 30 seconds however with parsing it takes longer than 10 minutes
* Summary No Yarn: ![alt](./README.md.resources/performance-results-no-yarn.png)
* Full Results can be found here [Google Sheet with results here](https://docs.google.com/spreadsheets/d/1rT22cXdM3pVAIEyy-oArSACXeq6O7MKxk-B1ycnBjFo/edit?usp=sharing)
  * for definition of scenarios refer to [application.conf#scenarios section](./src/main/resources/application.conf) 

# References
* [Maven](https://spark.apache.org)
* [Maven for scala](https://docs.scala-lang.org/tutorials/scala-with-maven.html)
* [Brew](https://brew.sh)
* [Hadoop](https://hadoop.apache.org)
* [Apache Spark](https://spark.apache.org)

# TODO
* fix auto coorect of HDFS & YARN config on EMR
