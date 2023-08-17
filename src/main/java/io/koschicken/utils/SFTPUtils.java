package io.koschicken.utils;

import org.apache.commons.vfs2.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Stream;

@Component
public class SFTPUtils {

    @Value("${sftp.remote-host}")
    private String remoteHost;
    @Value("${sftp.remote-dir}")
    private String remoteDir;
    @Value("${sftp.local-dir}")
    private String localDir;
    @Value("${sftp.username}")
    private String username;
    @Value("${sftp.password}")
    private String password;
    @Value("${sftp.file-suffix}")
    private String fileSuffix;

    public List<String> ls() throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        FileObject remote = manager.resolveFile("sftp://" + username + ":" + password + "@" + remoteHost + "/" + remoteDir);
        FileObject[] children = remote.getChildren();
        return Stream.of(children).filter(fileObject -> fileObject.getName().getExtension().equals(fileSuffix))
                .map(fileObject -> fileObject.getName().getBaseName()).toList();
    }

    public void download(String filename, String subDir) throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        FileObject local = manager.resolveFile(localDir + subDir + filename);
        FileObject remote = manager.resolveFile("sftp://" + username + ":" + password + "@" + remoteHost + "/" + remoteDir + "/" + filename);
        local.copyFrom(remote, Selectors.SELECT_SELF);
        local.close();
        remote.close();
    }

    public void download(List<String> fileList, String subDir) throws FileSystemException {
        FileSystemManager manager = VFS.getManager();
        for (String filename : fileList) {
            FileObject local = manager.resolveFile(localDir + subDir + filename);
            FileObject remote = manager.resolveFile("sftp://" + username + ":" + password + "@" + remoteHost + "/" + remoteDir + "/" + filename);
            if (!local.exists()) {
                local.copyFrom(remote, Selectors.SELECT_SELF);
            }
            local.close();
            remote.close();
        }
    }
}
