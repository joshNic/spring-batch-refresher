package com.example.springbatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchApplication {

    /**
     * Start of ChunkBasedJobs
     */


    public static String[] tokens = new String[]{
            "order_id", "first_name", "last_name", "email", "cost", "item_id", "item_name", "ship_date"
    };
    public static String[] names = new String[]{
            "orderId", "firstName", "lastName", "email", "cost", "itemId", "itemName", "shipDate"
    };
    public static String ORDER_SQL = "select order_id,first_name," +
            "last_name,email,cost,item_id,item_name," +
            "ship_date from SHIPPED_ORDER order by order_id";

    public static String INSERT_ORDER_SQL = "insert into SHIPPED_ORDER_OUTPUT(order_id,first_name," +
            "last_name,email,item_id,item_name,cost," +
            "ship_date) values(?,?,?,?,?,?,?,?)";
    public static String INSERT_ORDER_SQL_NAMED_PARAMETERS = "insert into SHIPPED_ORDER_OUTPUT(order_id,first_name," +
            "last_name,email,item_id,item_name,cost," +
            "ship_date) values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate)";
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    public static void main(String[] args) {
        SpringApplication.run(SpringBatchApplication.class, args);
    }

    @Bean
    public Step packageItemStep() {
        return this.stepBuilderFactory.get("packageItemStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

                String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
                String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();

                System.out.println(String.format("The %s has been packaged on %s", item, date));
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step driveToAddressStep() {
        return this.stepBuilderFactory.get("driveToAddress").tasklet(new Tasklet() {

            final boolean GOT_LOST = false;

            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                if (GOT_LOST) throw new RuntimeException("Got lost driving to customer address");
                System.out.println("Successfully arrived at the address");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public JobExecutionDecider decider() {
        return new DeliveryDecider();
    }

    @Bean
    public JobExecutionDecider receiptDecider() {
        return new ReceiptDecider();
    }

    @Bean
    public Step storePackageStep() {
        return this.stepBuilderFactory.get("storePackageStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Storing the package while the customer address is being located ");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step thankCustomerStep() {
        return this.stepBuilderFactory.get("thankCustomerStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Correct Item delivered thanking customer ");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step refundCustomerStep() {
        return this.stepBuilderFactory.get("refundCustomerStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Incorrect Item delivered refunding customer");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public StepExecutionListener stepExecutionListener() {
        return new FlowerSelectionStepExecutionListener();
    }

    @Bean
    public Step selectFlowersStep() {
        return this.stepBuilderFactory.get("selectFlowersStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Gathering flowers for order");
                return RepeatStatus.FINISHED;
            }
        }).listener(stepExecutionListener()).build();
    }

    @Bean
    public Step arrangeFlowersStep() {
        return this.stepBuilderFactory.get("arrangeFlowersStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Arranging flowers for order");
                return RepeatStatus.FINISHED;
            }
        }).listener(stepExecutionListener()).build();
    }

    @Bean
    public Step removeThornsStep() {
        return this.stepBuilderFactory.get("removeThornsStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("remove thorns from roses");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step leaveAtDoorStep() {
        return this.stepBuilderFactory.get("leaveAtDoorStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Leaving the package at the door");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step nestedBillingJobStep() {
        return this.stepBuilderFactory.get("nestedBillingJobStep").job(billingJob()).build();
    }

    @Bean
    public Job billingJob() {
        return this.jobBuilderFactory.get("billingJob").start(sendInvoiceStep()).build();
    }

    @Bean
    public Step sendInvoiceStep() {
        return this.stepBuilderFactory.get("invoiceStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Invoice is sent to the customer");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step givePackageToCustomerStep() {
        return this.stepBuilderFactory.get("givePackageToCustomer").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Given the package to the customer ");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Flow billingFlow() {
        return new FlowBuilder<SimpleFlow>("billingFlow").start(sendInvoiceStep()).build();
    }

    @Bean
    public Job prepareFlowers() {
        return this.jobBuilderFactory.get("prepareFlowersJob")
                .start(selectFlowersStep())
                .on("TRIM_REQUIRED").to(removeThornsStep()).next(arrangeFlowersStep())
                .from(selectFlowersStep())
                .on("NO_TRIM_REQUIRED").to(arrangeFlowersStep())
                .from(arrangeFlowersStep()).on("*").to(deliveryFlow())
                .end()
                .build();
    }

    @Bean
    public Flow deliveryFlow() {
        return new FlowBuilder<SimpleFlow>("deliveryFlow").start(driveToAddressStep())
                .on("FAILED")
                .to(storePackageStep())
                .from(driveToAddressStep())
                .on("*")
                .to(decider())
                .on("PRESENT")
                .to(givePackageToCustomerStep())
                .next(receiptDecider()).on("CORRECT").to(thankCustomerStep())
                .from(receiptDecider()).on("INCORRECT").to(refundCustomerStep())
                .from(decider())
                .on("NOT_PRESENT")
                .to(leaveAtDoorStep()).build();
    }

    @Bean
    public ItemReader<String> itemReaderSimpleData() {
        return new SimpleItemReader();
    }

    @Autowired
    public DataSource dataSource;
    @Bean
    public ItemReader<Order> itemReaderDatabaseSingleThread(){
        return new JdbcCursorItemReaderBuilder<Order>().dataSource(dataSource)
                .name("jdbcCursorItemReader")
                .sql(ORDER_SQL)
                .rowMapper(new OrderRowMapper())
                .build();
    }

    @Bean
    public ItemReader<Order> itemReaderDatabaseMultiThread() throws Exception {
        return new JdbcPagingItemReaderBuilder<Order>()
                .dataSource(dataSource)
                .name("jdbcCursorItemReader")
                .queryProvider(queryProvider())
                .rowMapper(new OrderRowMapperMulti())
                .pageSize(10)
                .build();
    }

    @Bean
    public ItemWriter<Order> orderItemWriterDatabase(){
        return new JdbcBatchItemWriterBuilder<Order>()
                .dataSource(dataSource)
                .sql(INSERT_ORDER_SQL)
                .itemPreparedStatementSetter(new OrderItemPreparedStatementSetter())
                .build();
    }

    @Bean
    public ItemWriter<Order> orderItemWriterDatabaseParameters(){
        return new JdbcBatchItemWriterBuilder<Order>()
                .dataSource(dataSource)
                .sql(INSERT_ORDER_SQL_NAMED_PARAMETERS)
                .beanMapped()
                .build();
    }

    @Bean
    public PagingQueryProvider queryProvider() throws Exception {
        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setSelectClause("select order_id,first_name,last_name,email,cost,item_id,item_name,ship_date");
        factory.setFromClause("from SHIPPED_ORDER");
        factory.setSortKey("order_id");
        factory.setDataSource(dataSource);
        return factory.getObject();
    }

    @Bean
    public ItemReader<Order> orderItemReader() {
        FlatFileItemReader<Order> itemReader = new FlatFileItemReader<>();
        itemReader.setLinesToSkip(1);
        itemReader.setResource(new FileSystemResource("/Users/joshua/Desktop/EA/springBatch/shipped_orders.csv"));
        DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames(tokens);
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
        itemReader.setLineMapper(lineMapper);
        return itemReader;
    }

    @Bean
    public Step chunkBasedStep() {
        return this.stepBuilderFactory.get("chunkBasedStep").<String, String>chunk(3).reader(itemReaderSimpleData())
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        System.out.println(String.format("Recieved list of size: %s", list.size()));
                        list.forEach(System.out::println);
                    }
                }).build();
    }


    @Bean
    public Step orderChunkStep() {
        return this.stepBuilderFactory.get("orderChunkStep").<Order, Order>chunk(3)
                .reader(orderItemReader())
                .writer(new ItemWriter<Order>() {
                    @Override
                    public void write(List<? extends Order> list) throws Exception {
                        System.out.println(String.format("Recieved list of size: %s", list.size()));
                        list.forEach(System.out::println);
                    }
                }).build();
    }

    @Bean
    public Step orderChunkDatabaseSingleThreadStep() {
        return this.stepBuilderFactory.get("orderChunkDatabaseSingleThreadStep").<Order, Order>chunk(10)
                .reader(itemReaderDatabaseSingleThread())
                .writer(new ItemWriter<Order>() {
                    @Override
                    public void write(List<? extends Order> list) throws Exception {
                        System.out.println(String.format("Recieved list of size: %s", list.size()));
                        list.forEach(System.out::println);
                    }
                }).build();
    }

    @Bean
    public Step orderChunkDatabaseMultiThreadStep() throws Exception {
        return this.stepBuilderFactory.get("orderChunkDatabaseMultiThreadStep").<Order, Order>chunk(10)
                .reader(itemReaderDatabaseMultiThread())
                .writer(new ItemWriter<Order>() {
                    @Override
                    public void write(List<? extends Order> list) throws Exception {
                        System.out.println(String.format("Recieved list of size: %s", list.size()));
                        list.forEach(System.out::println);
                    }
                }).build();
    }


    @Bean
    public Step orderChunkWriterStep() throws Exception {
        return this.stepBuilderFactory.get("orderChunkWriterStep").<Order, Order>chunk(10)
                .reader(itemReaderDatabaseMultiThread())
                .writer(itemWriter()).build();
    }

    @Bean
    public Step orderChunkWriterDatabaseStep() throws Exception {
        return this.stepBuilderFactory.get("orderChunkWriterDatabaseStep").<Order, Order>chunk(10)
                .reader(itemReaderDatabaseMultiThread())
                .writer(orderItemWriterDatabase()).build();
    }

    @Bean
    public ItemWriter<Order> itemWriter() {
        FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<>();

        itemWriter.setResource(new FileSystemResource("/Users/joshua/Desktop/EA/springBatch/shipped_orders_output.csv"));
        DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");

        BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(names);
        aggregator.setFieldExtractor(fieldExtractor);
        itemWriter.setLineAggregator(aggregator);
        return itemWriter;
    }

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("job").start(chunkBasedStep()).build();
    }

    @Bean
    public Job orderJob() {
        return this.jobBuilderFactory.get("orderJob").start(orderChunkStep()).build();
    }

    @Bean
    public Job orderDatabaseSingleThreadJob() {
        return this.jobBuilderFactory.get("orderDatabaseSingleThreadJob").start(orderChunkDatabaseSingleThreadStep()).build();
    }

    @Bean
    public Job orderDatabaseMultiThreadJob() throws Exception {
        return this.jobBuilderFactory.get("orderDatabaseMultiThreadJob").start(orderChunkDatabaseMultiThreadStep()).build();
    }

    @Bean
    public Job orderWriterJob() throws Exception {
        return this.jobBuilderFactory.get("orderWriterJob").start(orderChunkWriterStep()).build();
    }

    @Bean
    public Job orderWriterDatabaseJob() throws Exception {
        return this.jobBuilderFactory.get("orderWriterDatabaseJob").start(orderChunkWriterDatabaseStep()).build();
    }

    /**
     * End of ChunkBasedJobs
     */


    @Bean
    public Job deliverPackageJob() {
        return this.jobBuilderFactory.get("deliverPackageJob")
                .start(packageItemStep())
                .split(new SimpleAsyncTaskExecutor())
                .add(deliveryFlow(), billingFlow())
//                .on("*").to(deliveryFlow())
//                .next(nestedBillingJobStep())
                .end()
                .build();
    }

}
