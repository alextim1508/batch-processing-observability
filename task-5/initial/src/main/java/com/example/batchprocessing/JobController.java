package com.example.batchprocessing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobController {

    private final JobLauncher jobLauncher;

    private final Job importProductJob;

    public JobController(JobLauncher jobLauncher, Job importProductJob) {
        this.jobLauncher = jobLauncher;
        this.importProductJob = importProductJob;
    }

    @PostMapping("/launchJob")
    public ResponseEntity<String> launchJob() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("time", System.currentTimeMillis())
                    .toJobParameters();

            JobExecution run = jobLauncher.run(importProductJob, jobParameters);
            return ResponseEntity.ok("Job launched with ID: " + run.getJobId());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Job failed: " + e.getMessage());
        }
    }
}