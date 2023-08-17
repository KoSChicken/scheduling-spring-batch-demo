package io.koschicken.controller;

import io.koschicken.job.DownloadFileJob;
import io.koschicken.job.ImportTradeJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchJobController {

    @Autowired
    private DownloadFileJob downloadFileJob;
    @Autowired
    private ImportTradeJob importTradeJob;

    @PostMapping("downloadFile")
    String downloadFile() {
        downloadFileJob.launch();
        return "OK";
    }

    @PostMapping("importTrade")
    String importTrade() throws Exception {
        importTradeJob.launch();
        return "OK";
    }
}
