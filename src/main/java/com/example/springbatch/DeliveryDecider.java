package com.example.springbatch;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import java.time.LocalDate;
import java.time.LocalDateTime;


public class DeliveryDecider implements JobExecutionDecider {
    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        String result = LocalDateTime.now().getHour() <12?"PRESENT":"NOT_PRESENT";
        System.out.printf("Decider result is: %s%n", result);
        return new FlowExecutionStatus(result);
    }
}
