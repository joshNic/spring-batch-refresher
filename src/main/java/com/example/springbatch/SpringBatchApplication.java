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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.List;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchApplication {

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
    public ItemReader<String> itemReader() {
        return new SimpleItemReader();
    }


    @Bean
    public Step chunkBasedStep() {
        return this.stepBuilderFactory.get("chunkBasedStep").<String, String>chunk(3).reader(itemReader())
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        System.out.println(String.format("Recieved list of size: %s", list.size()));
                        list.forEach(System.out::println);
                    }
                }).build();
    }

    @Bean
    public Job job() {
        return this.jobBuilderFactory.get("job").start(chunkBasedStep()).build();
    }


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
