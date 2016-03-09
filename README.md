
* [Leiothrix是什么](#what)
* [User Guide](#userguide)

# <a name="what">Leiothrix是什么</a>

现在是大数据的时代，随着大数据技术的发展，以hadoop、spark为代表的离线大批量数据的计算框架已经被使用地越来越多，但它们都是以hdfs作为底层存储，也就是说，如要使用批量数据计算技术，首先要把所有的数据存储到hdfs中。但对于不少公司，尤其是创业公司，在发展的初期和中期，数据依然是以传统的关系型数据库(MySQL)为主，如果此时需要做些批量计算的工作，同时表数据还比较大，批量计算就成了一个耗时、容易出错的事情了，也就是说，缺乏一个为关系型数据库准备的分布式、高性能、高可用、容错性好的计算框架。

## 目前在MySQL上做批量计算，要怎么做？
举个例子:

假设现在有张表t_user(5000万条数据)，有个字段是身份证号，随着业务的发展，需要知道用户所属的省、市，这可以通过身份证号前6位，与全国区域规划编码表对照，得出其区域信息。由于身份证号是不变的，所以tech leader决定在t_user中增加两列:province和city，以避免每次都要重新根据身份证号计算。那么现在问题是：我们需要给t_user中的已有数据，赋予province和city这两个字段初始值.程序员小明接到了这个任务，他打算这么做

1. 新建工程或使用公司的模板工程来搭建开发环境，spring、mybatis这些引进来，dao、mapper生成好，小明对这些驾轻就熟，很快就完成了。
2. t_user表的数据这么多，肯定得多线程跑，并发去数据库查，然后算出province和city，更新到相应记录中去。在多线程前，要对数据分片，每个线程负责一个分片。多少个线程合适?得先看看线上资源怎么样，在测试的时候也得跟踪下CPU使用情况，再调整；每个数据片的起始点和结束点在哪？这块程序得好好测测，不然跑重了或跑漏了，都显示不出自己缜密的逻辑能力😊。小明的技术不错，也很快就写完了，对自己很满意。
3. 拿开发环境的小数据量测试了下，没什么问题，开始到线上跑。先跟运维申请台机器，如果使用的是云服务，申请也很快，来个16核32G的，按时付费。小明开始跑了，1秒钟能处理1万条数据，整个跑完，大概要1个半小时。
4. 跑了30分钟，突然报错了，程序退出，原来生产环境有身份证号为空的情况，于是改bug，并加上异常处理，以保证不会由于错误数据的原因导致系统挂掉，改完之后再跑。第二次，跑到60分钟的时候，程序跑不动了，原来程序有块造成了内存泄露，JVM满了，只能重启，再从头跑。为了避免再有这种情况，加上个防重复校验，即原来有值的，就不再跑了。第三次终于顺利了，成功结束。
5. 一切OK，通知运维释放资源。从写程序到成功实施完，大约花了1天时间。在1天时间内能做完这么多，并且解决不少问题，小明对自己还是很满意的。

过几天，小王所在的项目组，也遇到了类似这样的场景，他不知道小明已经写过类似的代码，即使知道了，也未必想看别人的代码，再来改，否则怎么显示出自己的编码水平呢！于是，小王又被上述问题折腾一通。

## 有哪些事情是可以不用重复开发的？
上面小明的这个案例，还是相对简单的，只读取1个字段，遇到的问题也不算多，所以跑完所有数据，耗时也不算长。如果耗时长的，晚上跑，第二天早上来一看，竟然挂了，要重跑，那简直人都要崩溃😡
但上面的很多事情，是框架级可以帮助解决的

* 多少个线程合适:框架可以根据物理机的当前资源来动态决定。
* 每个数据片的边界:框架自然也可以帮忙做到，并且不会错。
* 断点续跑:框架可以记录当前处理进度，以做到这一点。
* 跑的慢怎么办:这是最严重的问题，因为程序不可避免有bug，每改一个bug，需要从头再来，等上半小时、1小时才能跑完，这简直无法容忍！框架可以分布式地来解决，利用多台物理机来将计算能力从多线程扩散到多物理机/多进程，同时能够访问多台数据库server(现在的数据库，一般都是master-slave模式部署)，以提高数据获取的能力。
* 申请/释放资源:能否充分利用现有线上环境的空闲资源？尤其是供测试用的预发布环境，负载很低，资源过剩。框架可以做到。

**这些就是Leiothrix的诞生背景，以及它所做的事情**

#<a name="userguide">User Guide</a>
## 引入maven dependency
	       <dependency>
                <groupId>xin.bluesky</groupId>
                <artifactId>leiothrix-worker</artifactId>
                <version>0.1</version>
            </dependency>

## <A NAME="example">example</A>
	public class PrintDataExample {

    private static final Logger logger = LoggerFactory.getLogger(PrintDataExample.class);

    public static void main(String[] args) throws Exception {
        logger.info("开始执行{}", PrintDataExample.class.getSimpleName());

        // 解析从数据库获取来的数据
        WorkerConfiguration configuration=new WorkerConfiguration();
        configuration.setDatabasePageDataHandler(new DatabasePageDataHandler() {
            @Override
            public void handle(String tableName, List<JSONObject> dataList) throws Exception {
                dataList.forEach(data -> {
                    data.entrySet().forEach(entry -> {
                        logger.info(entry.getKey() + ":" + entry.getValue());
                    });
                });
            }

            // 数据查询发生异常时的处理
            @Override
            public void exceptionCaught(String tableName, List<JSONObject> dataList, Exception e) {
                logger.error("处理表[{}]数据时发生异常,数据为:{},异常为:{}",
                        tableName, CollectionsUtils2.toString(dataList), ExceptionUtils.getStackTrace(e));
            }
        });

        // 创建worker并启动
        WorkerProcessor bootStrap = new WorkerProcessor(configuration);

        bootStrap.start();
      }
    }

   任务所连接的数据源，读取的表/列，是通过json格式的配置文件指定的   

    {
   	"databaseInfoList": [
     {
      "dialect": "mysql",
      "ip": "192.168.5.233",
      "port": 3306,
      "schema": "gfb-usercenter",
      "userName": "root",
      "password": "123456"
     },
     {
      "dialect": "mysql",
      "ip": "121.43.177.8",
      "port": 3344,
      "schema": "gfb-usercenter",
      "userName": "gongfubao",
      "password": "dashugongfubao"
     }
    ],
    "tableList": [
     {
      "name": "t_user",
      "columns": "Surname,Mobile",
      "where": "1=1"
     },
     {
      "name": "t_user_login_history"
     }
    ],
    "rangeAllocator": "table_sequence"      

   		
## 部署
如果你之前部署过storm或者spark，那么就对Leiothrix的部署很熟悉了。
### 首先部署server
### 部署你的计算项目

1. 要把你的项目打成一个可执行的jar包，包括它所依赖的lib包，下面是使用maven-shade-plugin的做法:在pom.xml中增加如下这段

	```	
	<build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>2.4.3</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <filters>
                                <filter>
                                    <artifact>*:*</artifact>
                                    <excludes>
                                        <exclude>META-INF/*.SF</exclude>
                                        <exclude>META-INF/*.DSA</exclude>
                                        <exclude>META-INF/*.RSA</exclude>
                                        <exclude>META-INF/*.INF</exclude>
                                    </excludes>
                                </filter>
                            </filters>
                            <transformers>
                                <transformer
                                        implementation="org.apache.maven.plugins.shade.resource.ManifestResourc
eTransformer">                                    <mainClass>xin.bluesky.leiothrix.examples.print.PrintDataExample</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
    ```	
    	
2. 在执行mvn clean install打出你的项目jar包之后，下载leiothrix-0.1-bin.tar.gz并解压，进入bin目录，在mac/linux命令行终端中执行:

	```
	./submit.sh serverIp 12800 /path/to/jar /path/to/config.json mainClass
	```	
	举上面的[PrintExample](#example)来说,假设leiothrix-server部署的ip是192.168.100.1,port是默认的12800，工程jar包在~/example.jar,配置文件是~/example-config.json，计算工程的包名是com.x.y那么上述提交任务的完整命令就是
	
    	./submit.sh	192.168.100.1 12800 ~/example.jar ~/example-config.json com.x.y.PrintExample

3. 在命令行终端返回的信息中，可以看到提交的任务ID和状态