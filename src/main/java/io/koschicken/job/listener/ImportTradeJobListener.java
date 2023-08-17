package io.koschicken.job.listener;

import io.koschicken.job.domain.Trade;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * JobExecutionListener
 * 提供beforeJob和afterJob两个方法，可以在此处编写一些自定义逻辑，在作业开始前/结束后执行
 */
@Slf4j
@Component
public class ImportTradeJobListener implements JobExecutionListener {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public ImportTradeJobListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("import trade COMPLETED");
            jdbcTemplate.query("SELECT * FROM trade",
                    (rs, row) -> new Trade(
                            rs.getString(1),
                            rs.getLong(2),
                            rs.getBigDecimal(3),
                            rs.getString(4))
            ).forEach(trade -> log.info("Found <{{}}> in the database.", trade));
        } else {
            // 此处可以编写重启任务的逻辑
            log.info("import trade job FAILED");
        }
    }
}
