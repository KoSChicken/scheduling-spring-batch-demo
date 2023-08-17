package io.koschicken.job;

import io.koschicken.job.listener.DownloadFileJobListener;
import io.koschicken.job.tasklet.FileFetchTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.PlatformTransactionManager;

import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
@Configuration
public class DownloadFileJob {

    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private DownloadFileJobListener listener;
    @Autowired
    private FileFetchTask fileFetchTask;

    @Scheduled(fixedRate = 10000)
    public void launch() {
        Job job = downloadFile(jobRepository, listener, fetchFile(jobRepository, transactionManager, fileFetchTask));
        JobParameters param = new JobParametersBuilder()
                .addString("JobID", String.valueOf(System.currentTimeMillis()))
                .toJobParameters();
        try {
            jobLauncher.run(job, param);
        } catch (JobExecutionAlreadyRunningException e) {
            log.error("job {} already running", job.getName());
        } catch (JobRestartException e) {
            log.error("job {} can not restart", job.getName());
        } catch (JobInstanceAlreadyCompleteException e) {
            log.error("job {} already completed", job.getName());
        } catch (JobParametersInvalidException e) {
            log.error("job {} parameters invalid", job.getName());
        }
    }

    @Bean
    public Job downloadFile(JobRepository jobRepository, DownloadFileJobListener listener, TaskletStep fetchFile) {
        String jobName = format.format(new Date());
        return new JobBuilder(jobName, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(fetchFile)
                .build();
    }

    @Bean
    public TaskletStep fetchFile(JobRepository jobRepository,
                                 PlatformTransactionManager transactionManager, FileFetchTask fileFetchTask) {
        return new StepBuilder("fetchFile", jobRepository)
                .tasklet(fileFetchTask, transactionManager)
                .build();
    }
}
