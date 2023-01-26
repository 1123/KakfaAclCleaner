package io.confluent.examples.aclcleaner;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

// TODO: add a dryrun option

@Slf4j
public class AclCleanerService {

    private Admin adminClient;

    public AclCleanerService(String propertiesFile) throws IOException {
        Properties properties = new Properties();
        File file = new File(propertiesFile);
        InputStream stream = new FileInputStream(file);
        properties.load(stream);
        adminClient = Admin.create(properties);
    }

    @SneakyThrows
    public void clean(boolean dryrun) {
        var topicList = adminClient.listTopics().listings().get();
        log.info(topicList.toString());
        var aclList = adminClient.describeAcls(
                new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY)
        ).values().get();
        log.info(String.valueOf(aclList));
        Map<String, TopicListing> topicMap = new HashMap<>();
        topicList.forEach(topicListing -> {
            topicMap.put(topicListing.name(), topicListing);
        });
        log.info(topicMap.toString());
        aclList.forEach(aclBinding -> {
            // TODO: what about ACLs for consumer groups?
            // TODO: also think about ACLs for transactional producers
            // TODO: what about ACLs on cluster level?
            if (aclBinding.pattern().resourceType() == ResourceType.TOPIC) {
                if (aclBinding.pattern().name().equals("*")) {
                    // TODO: do we need to delete wildcards as well?
                    log.info("We are not deleting ACLs with wildcards");
                    return;
                }
                if (topicMap.get(aclBinding.pattern().name()) == null) {
                    log.info("Aclbinding for topic {} must be deleted {}", aclBinding.pattern().name(), aclBinding);
                    if (!dryrun) {
                        deleteAclBinding(aclBinding);
                    } else {
                        log.info("Not really deleting, since this is a dryrun.");
                    }
                }
            }
        });
    }

    @SneakyThrows
    private void deleteAclBinding(AclBinding aclBinding) {
        log.info("Deleting acl {}", aclBinding.toString());
        adminClient.deleteAcls(Collections.singletonList(
                new AclBindingFilter(
                        new ResourcePatternFilter(ResourceType.TOPIC, aclBinding.pattern().name(), PatternType.LITERAL),
                        AccessControlEntryFilter.ANY
                ))
        ).all().get();
    }

}
