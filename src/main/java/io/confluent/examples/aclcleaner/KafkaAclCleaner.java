package io.confluent.examples.aclcleaner;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class KafkaAclCleaner {

    @SneakyThrows
    public static void main(String ... args) {
        // TODO: there should be a start scirpt, to start this from the command line.
        // TODO: we need to package all dependencies in a Uber JAR for this.
        AclCleanerService aclCleanerService = new AclCleanerService(args[0]);
        if (Arrays.asList(args).contains("--dry-run")) {
            aclCleanerService.clean(true);
        } else {
            aclCleanerService.clean(false);
        }
    }

}
