package io.confluent.examples.aclcleaner;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AclCleanerTest {

    @BeforeEach
    public void setup() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "  username=\"kafka\" " +
                "  password=\"kafka-pass\";");
        properties.put("bootstrap.servers", "localhost:9093");
        Admin adminClient = Admin.create(properties);
        var createAclResult = adminClient.createAcls(Collections.singletonList(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, "test-topic", PatternType.LITERAL),
                        new AccessControlEntry("User:consumer", "*", AclOperation.CREATE, AclPermissionType.ALLOW)
                )
        )).all().get();
    }

    @Test
    public void testAclCleaning() throws IOException {
        AclCleanerService aclCleanerService = new AclCleanerService("/tmp/aclcleaner.properties");
        aclCleanerService.clean(true);
        // TODO: assert that the ACL was deleted
    }

    // TODO: add test case that shows that ACLs are NOT deleted for existing topics
    // TODO: think about more test cases
}

