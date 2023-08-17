package io.koschicken.job.listener;

import io.koschicken.job.ImportTradeJob;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * JobExecutionListener
 * 提供beforeJob和afterJob两个方法，可以在此处编写一些自定义逻辑，在作业开始前/结束后执行
 */
@Slf4j
@Component
public class DownloadFileJobListener implements JobExecutionListener {

    private final ImportTradeJob importTradeJob;

    @Autowired
    public DownloadFileJobListener(ImportTradeJob importTradeJob) {
        this.importTradeJob = importTradeJob;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            try {
                log.info("File download COMPLETED, starting import job");
                importTradeJob.launch();
            } catch (Exception e) {
                log.error("importUser error", e);
            }
        } else {
            // 此处可以编写重启任务的逻辑
            log.info("File download job FAILED");
        }
    }
}
