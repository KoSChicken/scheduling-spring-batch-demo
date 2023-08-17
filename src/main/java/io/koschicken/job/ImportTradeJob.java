package io.koschicken.job;

import io.koschicken.job.domain.Trade;
import io.koschicken.job.listener.ImportTradeJobListener;
import com.thoughtworks.xstream.security.ExplicitTypePermission;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.builder.MultiResourceItemReaderBuilder;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.PathResource;
import org.springframework.core.io.Resource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
@Configuration
public class ImportTradeJob {

    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private ImportTradeJobListener listener;
    @Autowired
    private DataSource dataSource;

    @Value("${sftp.local-dir}")
    private String localDir;
    @Value("${sftp.file-suffix}")
    private String fileSuffix;

    public void launch() throws Exception {
        String today = format.format(new Date());
        String jobName = "importTrade-" + today;
        log.debug("NEW Job, name: {}", jobName);
        boolean checked = checkJob(jobName);
        if (checked) {
            log.info("Job Started at : {}", new Date());
            JobParameters param = new JobParametersBuilder()
                    .addString("date", today, false)
                    .toJobParameters();
            Step step = importMultiFileJob(jobRepository, transactionManager, tradeWriter(dataSource));
            Job job = importTrade(jobName, jobRepository, listener, step);
            JobExecution execution = jobLauncher.run(job, param);
            log.info("Job finished with status :{}", execution.getStatus());
        } else {
            log.info("Job already finished.");
        }
    }

    private boolean checkJob(String jobName) {
        JobExecution lastJobExecution = jobRepository.getLastJobExecution(jobName, new JobParametersBuilder().toJobParameters());
        return Objects.isNull(lastJobExecution) || lastJobExecution.getStatus() != BatchStatus.COMPLETED;
    }

    public Job importTrade(String jobName,
                           JobRepository jobRepository, ImportTradeJobListener listener,
                           Step step) {
        return new JobBuilder(jobName, jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(step)
                .build();
    }

    public Step importMultiFileJob(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                   JdbcBatchItemWriter<Trade> tradeWriter) throws FileSystemException {
        return new StepBuilder("importMultiFileJob", jobRepository)
                .<Trade, Trade>chunk(10, transactionManager)
                .reader(multiResourceXMLReader())
                .writer(tradeWriter)
                .build();
    }

    public MultiResourceItemReader<Trade> multiResourceXMLReader() throws FileSystemException {
        return new MultiResourceItemReaderBuilder<Trade>()
                .name("multiResourceXMLReader")
                .delegate(itemReader())
                .resources(listLocalFiles())
                .build();
    }

    public StaxEventItemReader<Trade> itemReader() {
        return new StaxEventItemReaderBuilder<Trade>()
                .name("itemReader")
                .addFragmentRootElements("trade")
                .unmarshaller(tradeMarshaller())
                .build();

    }

    private Resource[] listLocalFiles() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        String today = format.format(new Date());
        FileObject local = manager.resolveFile(localDir + today + "/");
        if (!local.exists() || !local.isFolder()) {
            local.createFolder();
        }
        List<PathResource> list = Stream.of(local.getChildren()).filter(fileObject -> fileObject.getName().getExtension().equals(fileSuffix))
                .map(fileObject -> new PathResource(fileObject.getPath())).toList();
        if (list.isEmpty()) {
            log.error("no resource");
        }
        return list.toArray(new Resource[0]);
    }

    @Bean
    public XStreamMarshaller tradeMarshaller() {
        Map<String, Class> aliases = new HashMap<>();
        aliases.put("trade", Trade.class);
        aliases.put("price", BigDecimal.class);
        aliases.put("isin", String.class);
        aliases.put("customer", String.class);
        aliases.put("quantity", Long.class);
        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(aliases);
        ExplicitTypePermission typePermission = new ExplicitTypePermission(new Class[]{Trade.class});
        marshaller.setTypePermissions(typePermission);
        return marshaller;
    }

    @Bean
    public JdbcBatchItemWriter<Trade> tradeWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Trade>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO trade (isin, quantity, price, customer) VALUES (:isin, :quantity, :price, :customer)")
                .dataSource(dataSource)
                .build();
    }
}
