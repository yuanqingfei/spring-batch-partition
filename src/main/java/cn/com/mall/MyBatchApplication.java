package cn.com.mall;

import cn.com.mall.pojo.OfbizProduct;
import cn.com.mall.pojo.RedisProduct;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.log4j.Logger;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
@PropertySource({"classpath:jdbc.properties", "classpath:redis.properties"})
public class MyBatchApplication {

    private static final Logger logger = Logger.getLogger(MyBatchApplication.class);

    @Value("${jdbc.url}")
    public String jdbcUrl;

    @Value("${jdbc.username}")
    public String jdbcUserName;

    @Value("${jdbc.password}")
    public String jdbcPassword;

    @Value("${redis.host}")
    public String redisHost;

    @Value("${redis.port}")
    public String redisPort;

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    // jsut for init data
    @Bean
    protected Tasklet tasklet() {

        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext context) {

                JdbcOperations jdbcTemplate = new JdbcTemplate(druidDataSource());

                for (int i = 0; i < 10; i++) {
                    String productCategory = "xxx" + i;
                    for (int j = 0; j < 7; j++) {
                        String productName = "yyy" + j;
                        String sql = "insert into my_product(name, category) values('" + productName + "','" + productCategory + "');";
                        System.out.println(sql);
                        jdbcTemplate.execute(sql);
                    }
                }

                return RepeatStatus.FINISHED;
            }
        };

    }

    @Bean
    public RedisConnectionFactory connectionFactory() {
//	  RedisSentinelConfiguration sentinelConfig = new RedisSentinelConfiguration()
//		  .master("masterSession")
//		  .sentinel("172.29.165.244", 26379)
//		  .sentinel("172.29.165.245", 26379);
//	  return new JedisConnectionFactory(sentinelConfig);

        JedisConnectionFactory factory = new JedisConnectionFactory();
        factory.setHostName(redisHost);
        factory.setPort(Integer.parseInt(redisPort));

        return factory;
    }

    @Bean
    public DruidDataSource druidDataSource() {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(jdbcUrl);
        ds.setPassword(jdbcPassword);
        ds.setUsername(jdbcUserName);
        return ds;
    }

    @Primary
    @Bean
    @ConfigurationProperties(prefix = "datasource.hsql")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public BatchConfigurer configurer(@Qualifier("dataSource") DataSource batchDataSource) {
        return new DefaultBatchConfigurer(batchDataSource);
    }

    @Bean
    public StringRedisTemplate stringTemplate(RedisConnectionFactory redisConnectionFactory) {
        StringRedisTemplate stringTemplate = new StringRedisTemplate();
        stringTemplate.setConnectionFactory(redisConnectionFactory);
        return stringTemplate;
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<OfbizProduct> itemReader(@Value("#{stepExecutionContext['category']}") String brandNo, @Qualifier("druidDataSource") DataSource ds) {
        System.out.println("BrandNo: " + brandNo);
        JdbcCursorItemReader<OfbizProduct> itemReader = new JdbcCursorItemReader<OfbizProduct>();
        ;
        itemReader.setDataSource(ds);
        itemReader.setSql("select id, name, category from my_product where category = ?");
        itemReader.setRowMapper(new RowMapper<OfbizProduct>() {
            @Override
            public OfbizProduct mapRow(ResultSet rs, int rowNum) throws SQLException {
                OfbizProduct user = new OfbizProduct();
                user.setId(rs.getString("id"));
                user.setName(rs.getString("name"));
                user.setCategory(rs.getString("category"));
                logger.info("USER: " + user);
                return user;
            }

            ;

        });
        ArgumentPreparedStatementSetter setter = new ArgumentPreparedStatementSetter(new String[]{brandNo});
        itemReader.setPreparedStatementSetter(setter);

//		itemReader.setSaveState(false);
        return itemReader;
    }

    @Bean
    public ItemProcessor<OfbizProduct, RedisProduct> itemProcessor() {
        return new ItemProcessor<OfbizProduct, RedisProduct>() {

            @Override
            public RedisProduct process(OfbizProduct item) throws Exception {
                RedisProduct user = new RedisProduct();
                user.setKey(item.getId());
                user.setValue(item.getName() + "_" + item.getCategory());
                return user;
            }

        };
    }

    @Bean
    public ItemWriter<RedisProduct> itemWriter(final StringRedisTemplate template) {
        return new ItemWriter<RedisProduct>() {

            @Override
            public void write(List<? extends RedisProduct> items) throws Exception {
                for (RedisProduct user : items) {
                    template.opsForValue().set(user.getKey(), user.getValue());
                }
            }
        };
    }


    @Bean
    protected Job databaseReadingPartitioningJob(@Qualifier("partitionedStep") Step partitionedStep, @Qualifier("step2") Step step2) {
        return jobs.get("databaseReadingPartitioningJob1")
                .start(partitionedStep)
//                .start(step2)
                .build();
    }

    @Bean
    protected Step partitionedStep(Partitioner partitioner, PartitionHandler handler) {
        return steps.get("partitionedStep")
                .partitioner("readWritePartitionedStep_Slave", partitioner)
                .partitionHandler(handler)
                .build();
    }

    @Bean
    protected PartitionHandler handler(@Qualifier("readWritePartitionedStep") Step readWritePartitionedStep, TaskExecutor taskExecutor) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setTaskExecutor(taskExecutor);
        handler.setStep(readWritePartitionedStep);
//		handler.setGridSize(2);
        return handler;
    }

    @Bean
    protected TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10);
        return taskExecutor;
    }

    @Bean
    protected Partitioner partitioner(@Qualifier("druidDataSource") DataSource ds) {
        CategoryPartitioner brandPartitioner = new CategoryPartitioner(ds);
        return brandPartitioner;
    }


    @Bean
    protected Step readWritePartitionedStep(ItemReader<OfbizProduct> reader, ItemProcessor<OfbizProduct, RedisProduct> processor, ItemWriter<RedisProduct> writer) {
        return steps.get("readWritePartitionedStep")
                .<OfbizProduct, RedisProduct>chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    protected Step step2(Tasklet tasklet) {
        return steps.get("step2")
                .tasklet(tasklet)
                .build();
    }

    public static void main(String[] args) throws Exception {
        // System.exit is common for Batch applications since the exit code can
        // be used to drive a workflow
        System.exit(SpringApplication.exit(SpringApplication.run(MyBatchApplication.class, args)));
    }

}



