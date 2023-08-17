package io.koschicken.utils;

import org.apache.commons.vfs2.FileSystemException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class SFTPUtilsTest {

    @Autowired
    private SFTPUtils sftpUtils;

    @Test
    void ls() throws FileSystemException {
        List<String> list = sftpUtils.ls();
        list.forEach(System.out::println);
    }

    @Test
    void download() {
    }
}
