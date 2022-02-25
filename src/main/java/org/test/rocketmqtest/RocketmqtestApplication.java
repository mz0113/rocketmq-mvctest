package org.test.rocketmqtest;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.spring.autoconfigure.RocketMQAutoConfiguration;
import org.apache.rocketmq.spring.autoconfigure.RocketMQProperties;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

@SpringBootApplication
@ImportResource(locations={"classpath:spring-rocketmq.xml"})
public class RocketmqtestApplication {

	public static void main(String[] args) {
		SpringApplication.run(RocketmqtestApplication.class, args);
	}

	@Bean(RocketMQAutoConfiguration.PRODUCER_BEAN_NAME)
	@ConditionalOnMissingBean(DefaultMQProducer.class)
	@ConditionalOnProperty(prefix = "rocketmq", value = {"name-server", "producer.group"})
	public DefaultMQProducer defaultMQProducer(RocketMQProperties rocketMQProperties) {
		RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
		String nameServer = rocketMQProperties.getNameServer();
		String groupName = producerConfig.getGroup();
		Assert.hasText(nameServer, "[rocketmq.name-server] must not be null");
		Assert.hasText(groupName, "[rocketmq.producer.group] must not be null");

		String accessChannel = rocketMQProperties.getAccessChannel();

		String ak = rocketMQProperties.getProducer().getAccessKey();
		String sk = rocketMQProperties.getProducer().getSecretKey();
		boolean isEnableMsgTrace = rocketMQProperties.getProducer().isEnableMsgTrace();
		String customizedTraceTopic = rocketMQProperties.getProducer().getCustomizedTraceTopic();

		DefaultMQProducer producer = RocketMQUtil.createDefaultMQProducer(groupName, ak, sk, isEnableMsgTrace, customizedTraceTopic);

		producer.setNamesrvAddr(nameServer);
		if (!StringUtils.isEmpty(accessChannel)) {
			producer.setAccessChannel(AccessChannel.valueOf(accessChannel));
		}
		producer.setSendMsgTimeout(producerConfig.getSendMessageTimeout());
		producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
		producer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
		producer.setMaxMessageSize(producerConfig.getMaxMessageSize());
		producer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMessageBodyThreshold());
		producer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryNextServer());
/*		producer.setUseTLS(producerConfig.isTlsEnable());
		producer.setNamespace(producerConfig.getNamespace());*/
		producer.setSendLatencyFaultEnable(true);
		return producer;
	}

}
