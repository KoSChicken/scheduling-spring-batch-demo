package io.koschicken.job.tasklet;

import io.koschicken.utils.SFTPUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.vfs2.FileSystemException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Slf4j
@Component
public class FileFetchTask implements Tasklet {

    @Autowired
    private SFTPUtils sftpUtils;

    @Value("${sftp.local-dir}")
    private String localDir;

    @Override
    public RepeatStatus execute(@NonNull StepContribution contribution, @NonNull ChunkContext chunkContext) {
        String subDir = new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "/";
        createDirIfNotExist(subDir);
        try {
            List<String> fileList = sftpUtils.ls();
            sftpUtils.download(fileList, subDir);
        } catch (FileSystemException e) {
            throw new UnexpectedJobExecutionException("无法下载文件");
        }
        return RepeatStatus.FINISHED;
    }

    private void createDirIfNotExist(String subDir) {
        File file = new File(localDir + subDir);
        if (!file.exists() || !file.isDirectory()) {
            boolean mkdir = file.mkdir();
            if (!mkdir) {
                throw new UnexpectedJobExecutionException("无法创建任务文件夹，" + file.getPath());
            }
        }
    }
}
