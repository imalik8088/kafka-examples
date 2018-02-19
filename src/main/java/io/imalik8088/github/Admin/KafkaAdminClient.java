package io.imalik8088.github.Admin;


import kafka.security.auth.Topic;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAdminClient.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL);


        try (AdminClient adminClient = AdminClient.create(props)) {

            System.out.println("########## DESCRIBE CLUSTER ##########");
            final DescribeClusterResult desbCluster = adminClient.describeCluster();
            System.out.println(desbCluster.nodes().get());
            // logger.info("Describe {}", desbCluster.nodes().get());

            System.out.println("\n########## CREATE NEW TOPIC ##########");
            final NewTopic createNewTopic = new NewTopic(KafkaProperties.NEW_TOPIC, 2, (short) 1);
            adminClient.createTopics(Collections.singleton(createNewTopic));
            System.out.println(createNewTopic.toString());


            System.out.println("\n########## ALTER CONFIG ##########");

            ConfigEntry compression = new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            Map<ConfigResource, Config> updateConfig = new HashMap<ConfigResource, Config>();
            updateConfig.put(new ConfigResource(ConfigResource.Type.TOPIC, KafkaProperties.NEW_TOPIC), new Config(Collections.singleton(compression)));
            AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(updateConfig);
            alterConfigsResult.all();

            System.out.println("\n########## DESCRIBE TOPIC CONFIGS ##########");
            final Set<ConfigResource> config = Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, KafkaProperties.NEW_TOPIC));
            final DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(config);
            final Collection<Config> values = describeConfigsResult.all().get().values();
            for (Config c : values) {
                for (ConfigEntry ce: c.entries()) {
                    System.out.println(ce.toString());
                }
            }


            System.out.println("\n########## AVAILABLE TOPICS ##########");
            for (String topicName : adminClient.listTopics().names().get()) {
                System.out.println(topicName);
            }

            System.out.println("\n########## DESCRIBE TOPIC ##########");
            final DescribeTopicsResult describeTopics = adminClient.describeTopics(Collections.singleton(KafkaProperties.NEW_TOPIC));
            for (TopicDescription descTopic : describeTopics.all().get().values()) {
                System.out.println(String.format("Topicname=> %s \t System-Topic=> %s \t Partition size=> %d \nPartitions=> %s",
                        descTopic.name(), descTopic.isInternal(), descTopic.partitions().size(), descTopic.partitions().toString())
                );
            }

            System.out.println("\n########## DELETE TOPIC ##########");
            adminClient.deleteTopics(Collections.singleton(KafkaProperties.NEW_TOPIC));
            System.out.println("Now available=> " + adminClient.listTopics().names().get().toString());

            adminClient.close();
        }


    }
}
