

# 一、Spring Boot 入门

## Spring boot 2.3.0环境要求

JDK8-14

Spring5.2.6+

maven 3.3+

Gradle (6.3 or later). 5.6.x 也只支持，但是已过时



Spring Boot支持以下嵌入式servlet容器:

1. Tomcat 9.0
2. Jetty 9.4
3. Undertow 2.0
4. 任何Servlet 3.1+兼容的容器



## 安装Maven

spring boot 的groupId是org.springframework.boot

maven pom 文件需要继承 spring-boot-starter-parent



##  Spring Boot CLI

 Spring Boot CLI (Command Line Interface) 是一种命令行工具，您可以使用它来快速创建Spring的原型

 可运行[Groovy](http://groovy-lang.org/)  脚本

是启动Spring应用程序的最快方法

```
$ sdk install springboot dev /path/to/spring-boot/spring-boot-cli/target/spring-boot-cli-2.3.0.RELEASE-bin/spring-2.3.0.RELEASE/
$ sdk default springboot dev
$ spring --version
Spring CLI v2.3.0.RELEASE
```

创建app.groovy

```groovy
@RestController
class ThisWillActuallyRun {

    @RequestMapping("/")
    String home() {
        "Hello World!"
    }
}
```

运行脚本，即可启动spring boot应用

```
$ spring run app.groovy
```



## spring boot升级

当升级到一个新特性版本时，一些属性可能已经被重命名或删除。Spring Boot提供了一种在启动时分析应用程序环境和打印诊断的方法，还提供了在运行时临时迁移属性的方法。要启用该特性，请将以下依赖项添加到项目中:

```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-properties-migrator</artifactId>
    <scope>runtime</scope>
</dependency>
```



` mvn dependency:tree ` 将会打印项目依赖项的树表示形式。



@EnableAutoConfiguration

@EnableAutoConfiguration注解是类级别注解，加上该注解之后，spring boot会自动根据添加的jar依赖，添加相应的spring配置，如果加入了 `spring-boot-starter-web`依赖添加了Tomcat和Spring MVC，因此自动配置假设您正在开发web应用程序并相应地设置Spring。



## 创建可执行jar

加入 `spring-boot-maven-plugin` 到pom.xml文件中

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-maven-plugin</artifactId>
        </plugin>
    </plugins>
</build>
```

 执行`mvn package`生成可执行jar文件 

在target目录下，还有一个以.jar.original为后缀的文件，它是Maven在Spring引导重新打包之前创建的原始jar文件。



# 二、Spring Boot使用

## 启动器Starter

您不需要在构建配置中为任何这些依赖项提供版本，因为Spring Boot会为您管理它。当您升级Spring Boot本身时，这些依赖项也会以一致的方式升级。

### application starters

下面的应用程序启动器是Spring Boot提供的，在org.springframework.boot   group下。

| Name                                          | Description                                                  |
| :-------------------------------------------- | ------------------------------------------------------------ |
| `spring-boot-starter`                         | 核心启动器，包括自动配置支持、日志和YAML                     |
| `spring-boot-starter-activemq`                | 使用Apache ActiveMQ进行JMS消息传递的启动器                   |
| `spring-boot-starter-amqp`                    | 使用Spring AMQP and Rabbit MQ的启动器                        |
| `spring-boot-starter-aop`                     | 使用Spring AOP和AspectJ进行面向方面编程的启动器              |
| `spring-boot-starter-artemis`                 | 使用Apache Artemis实现JMS消息传递的启动器                    |
| `spring-boot-starter-batch`                   | Starter for using Spring Batch                               |
| `spring-boot-starter-cache`                   | Starter for using Spring Framework’s caching support         |
| `spring-boot-starter-data-cassandra`          | Starter for using Cassandra distributed database and Spring Data Cassandra |
| `spring-boot-starter-data-cassandra-reactive` | Starter for using Cassandra distributed database and Spring Data Cassandra Reactive |
| `spring-boot-starter-data-couchbase`          | Starter for using Couchbase document-oriented database and Spring Data Couchbase |
| `spring-boot-starter-data-couchbase-reactive` | Starter for using Couchbase document-oriented database and Spring Data Couchbase Reactive |
| `spring-boot-starter-data-elasticsearch`      | Starter for using Elasticsearch search and analytics engine and Spring Data Elasticsearch |
| `spring-boot-starter-data-jdbc`               | Starter for using Spring Data JDBC                           |
| `spring-boot-starter-data-jpa`                | Starter for using Spring Data JPA with Hibernate             |
| `spring-boot-starter-data-ldap`               | Starter for using Spring Data LDAP                           |
| `spring-boot-starter-data-mongodb`            | Starter for using MongoDB document-oriented database and Spring Data MongoDB |
| `spring-boot-starter-data-mongodb-reactive`   | Starter for using MongoDB document-oriented database and Spring Data MongoDB Reactive |
| `spring-boot-starter-data-neo4j`              | Starter for using Neo4j graph database and Spring Data Neo4j |
| `spring-boot-starter-data-r2dbc`              | Starter for using Spring Data R2DBC                          |
| `spring-boot-starter-data-redis`              | Starter for using Redis key-value data store with Spring Data Redis and the Lettuce client |
| `spring-boot-starter-data-redis-reactive`     | Starter for using Redis key-value data store with Spring Data Redis reactive and the Lettuce client |
| `spring-boot-starter-data-rest`               | Starter for exposing Spring Data repositories over REST using Spring Data REST |
| `spring-boot-starter-data-solr`               | Starter for using the Apache Solr search platform with Spring Data Solr |
| `spring-boot-starter-freemarker`              | Starter for building MVC web applications using FreeMarker views |
| `spring-boot-starter-groovy-templates`        | Starter for building MVC web applications using Groovy Templates views |
| `spring-boot-starter-hateoas`                 | Starter for building hypermedia-based RESTful web application with Spring MVC and Spring HATEOAS |
| `spring-boot-starter-integration`             | Starter for using Spring Integration                         |
| `spring-boot-starter-jdbc`                    | Starter for using JDBC with the HikariCP connection pool     |
| `spring-boot-starter-jersey`                  | Starter for building RESTful web applications using JAX-RS and Jersey. An alternative to [`spring-boot-starter-web`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-web) |
| `spring-boot-starter-jooq`                    | Starter for using jOOQ to access SQL databases. An alternative to [`spring-boot-starter-data-jpa`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-data-jpa) or [`spring-boot-starter-jdbc`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-jdbc) |
| `spring-boot-starter-json`                    | Starter for reading and writing json                         |
| `spring-boot-starter-jta-atomikos`            | Starter for JTA transactions using Atomikos                  |
| `spring-boot-starter-jta-bitronix`            | Starter for JTA transactions using Bitronix. Deprecated since 2.3.0 |
| `spring-boot-starter-mail`                    | Starter for using Java Mail and Spring Framework’s email sending support |
| `spring-boot-starter-mustache`                | Starter for building web applications using Mustache views   |
| `spring-boot-starter-oauth2-client`           | Starter for using Spring Security’s OAuth2/OpenID Connect client features |
| `spring-boot-starter-oauth2-resource-server`  | Starter for using Spring Security’s OAuth2 resource server features |
| `spring-boot-starter-quartz`                  | Starter for using the Quartz scheduler                       |
| `spring-boot-starter-rsocket`                 | Starter for building RSocket clients and servers             |
| `spring-boot-starter-security`                | Starter for using Spring Security                            |
| `spring-boot-starter-test`                    | Starter for testing Spring Boot applications with libraries including JUnit, Hamcrest and Mockito |
| `spring-boot-starter-thymeleaf`               | Starter for building MVC web applications using Thymeleaf views |
| `spring-boot-starter-validation`              | Starter for using Java Bean Validation with Hibernate Validator |
| `spring-boot-starter-web`                     | Starter for building web, including RESTful, applications using Spring MVC. Uses Tomcat as the default embedded container |
| `spring-boot-starter-web-services`            | Starter for using Spring Web Services                        |
| `spring-boot-starter-webflux`                 | Starter for building WebFlux applications using Spring Framework’s Reactive Web support |
| `spring-boot-starter-websocket`               | Starter for building WebSocket applications using Spring Framework’s WebSocket support |

### production starters

| Name                           | Description                                                  |
| :----------------------------- | :----------------------------------------------------------- |
| `spring-boot-starter-actuator` | 使用Spring Boot 执行器的启动器，它提供了可用于生产的特性，帮助您监视和管理应用程序 |

### technical starters

| Name                                | Description                                                  |
| ----------------------------------- | :----------------------------------------------------------- |
| `spring-boot-starter-jetty`         | 使用Jetty作为嵌入式servlet容器的启动器。另一种选择是 [`spring-boot-starter-tomcat`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-tomcat) |
| `spring-boot-starter-log4j2`        | 使用Log4j2进行日志记录的启动器。另一种选择是 [`spring-boot-starter-logging`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-logging) |
| `spring-boot-starter-logging`       | Starter for logging using Logback. 默认的logging 启动器      |
| `spring-boot-starter-reactor-netty` | 使用Reactor Netty作为嵌入式响应HTTP服务器的启动器。          |
| `spring-boot-starter-tomcat`        | 使用Tomcat作为嵌入式servlet容器的启动器。 [`spring-boot-starter-web`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-web)使用的默认servlet容器启动器 |
| `spring-boot-starter-undertow`      | 使用Undertow作为嵌入式servlet容器的启动器。另一种选择是 [`spring-boot-starter-tomcat`](https://docs.spring.io/spring-boot/docs/2.3.0.RELEASE/reference/htmlsingle/#spring-boot-starter-tomcat) |



## 主应用程序类

建议将主应用程序类定位在根包中，位于其他类之上。

@SpringBootApplication注释通常放在主类上，它隐式地为某些项定义了一个基本搜索包。

```java
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
```

`@SpringBootApplication` 包含下面3个特性：

- `@EnableAutoConfiguration`
- `@ComponentScan`
- `@Configuration`

如果不想在应用程序中使用组件扫描或配置属性扫描，可以使用下面方式：

```java
@Configuration(proxyBeanMethods = false)
@EnableAutoConfiguration
@Import({ MyConfig.class, MyAnotherConfig.class })
public class Application {

    public static void main(String[] args) {
            SpringApplication.run(Application.class, args);
    }

}
```



## 配置类

尽管可以将SpringApplication与XML源一起使用，但我们通常建议您的主源是一个@Configuration类。通常，定义main方法的类是首选的@Configuration类。

### 导入其他的配置类

不需要将所有的@Configuration放到一个类中。可以使用@Import注释导入其他配置类。或者，您可以使用@ComponentScan自动获取所有Spring组件，包括@Configuration类。

### 导入XML配置

如果必须使用基于XML的配置，仍然可以使用@Configuration类。然后用@ImportResource注解来加载XML配置文件。



## 自动配置

需要通过向一个@Configuration类添加@EnableAutoConfiguration或@SpringBootApplication注释来选择自动配置。

**自动配置是非侵入性的**。自定义的配置会替换自动配置的特定部分。例如，如果添加自己的数据源bean，默认的嵌入式数据库支持就会退出。

**使用——debug开关启动应用程序**

可了解当前正在应用的自动配置和原因，还可以为选择的核心日志记录器启用调试日志，并将条件报告记录到控制台。

**禁用特定的自动配置类**

1. 使用@SpringBootApplication的exclude属性来禁用特定的自动配置类

```java
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class})
public class MyApplication {
}
```

如果类不在类路径上，则可以使用注释的excludeName属性，并指定完全限定名。

2. 使用@EnableAutoConfiguration的exclude和excludeName属性
3. 使用`spring.autoconfigure.exclude` 排除自动配置类列表。

## 依赖注入

如果将应用程序类定位在根包中，添加@ComponentScan，不需要配置任何参数。所有应用程序组件(@Component、@Service、@Repository、@Controller等)都会自动注册为Spring bean。

使用@Autowired注解注入依赖

```java
	@Autowired
    public DatabaseAccountService(RiskAssessor riskAssessor) {
        this.riskAssessor = riskAssessor;
    }
```

如果bean有一个构造函数，您可以省略@Autowired，如下面的例子所示：

```java
@Service
public class DatabaseAccountService implements AccountService {

    private final RiskAssessor riskAssessor;

    public DatabaseAccountService(RiskAssessor riskAssessor) {
        this.riskAssessor = riskAssessor;
    }
}
```

其中riskAssessor字段可以标记为final，指定以后不能更改它。



## 运行应用程序

1. 作为打包应用程序运行

   ```
   $ java -jar target/myapplication-0.0.1-SNAPSHOT.jar
   ```

   还可以运行启用了远程调试支持的打包应用程序。这样做可以将调试器附加到打包的应用程序中

   ```
   $ java -Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=8000,suspend=n \
          -jar target/myapplication-0.0.1-SNAPSHOT.jar
   ```

2. 使用maven插件

   ```
   $ java -jar target/myapplication-0.0.1-SNAPSHOT.jar
   ```

   使用JAVA OPTS操作系统环境变量

   ```
   $ export MAVEN_OPTS=-Xmx1024m
   ```

3. 使用Gradle插件

   ```
   $ gradle bootRun
   ```

   ```
   使用JAVA OPTS操作系统环境变量
   $ export JAVA_OPTS=-Xmx1024m
   ```



## Developer Tools

Maven

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-devtools</artifactId>
        <optional>true</optional>
    </dependency>
</dependencies>
```

Gradle

```groovy
dependencies {
    developmentOnly("org.springframework.boot:spring-boot-devtools")
}
```

1. 如果想要禁用devtools ，可使用excluding  devtools或者设置-Dspring.devtools.restart.enabled=false system属性

2. 在Maven中将依赖标记为optional ，或者在Gradle中使用developmentOnly配置(如上所示)，可以防止devtools被传递到其他使用您项目的模块。

3. 重新打包在默认情况下不包含devtools。如果你想使用一个特定的远程devtools特性，在使用Maven插件时，将excludeDevtools属性设置为false。当使用Gradle插件时，配置任务的类路径以包含开发配置。

### Property Defaults

spring-boot-devtools会提供一些缺省配置，方便开发时使用。

**1. 禁用缓存**

 Spring boot有一些库使用缓存来提高性能，spring-boot-devtools禁用缓存，可以及时看到应用程序中所做的更改。

 缓存选项通常在application.properties中设置配置

**2. 调试日志**

spring-boot-devtools会自动为web日志组启用调试日志记录

也可手动配置`spring.mvc.log-request-details` 和 `spring.codec.log-request-details` 属性，记录所有请求日志。

### 自动重启

每当类路径上的文件发生变化时，使用spring-boot-devtools的应用程序就会自动重启。

某些资源(如静态资产和视图模板)修改不需要重新启动应用程序。



