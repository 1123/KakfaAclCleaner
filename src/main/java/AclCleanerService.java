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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class AclCleanerService {

    private Admin adminClient;

    public AclCleanerService() {
        // TODO: Configurionat should be read from a properites file
        Properties properties = new Properties();
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "  username=\"kafka\" " +
                "  password=\"kafka-pass\";");
        properties.put("bootstrap.servers", "localhost:9093");
        adminClient = Admin.create(properties);
    }

    @SneakyThrows
    public void clean() {
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
            if (aclBinding.pattern().resourceType() == ResourceType.TOPIC) {
                if (aclBinding.pattern().name().equals("*")) {
                    log.info("We are not deleting ACLs with wildcards");
                    return;
                }
                if (topicMap.get(aclBinding.pattern().name()) == null) {
                    log.info("Aclbinding must be deleted");
                    deleteAclBinding(aclBinding);
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
