package cn.gp1996.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    // kafka日志主题
    private final String KAFKA_ODS_LOG_TOPIC = "ods_base_log_test";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test() {
        return "success";
    }

    @RequestMapping("applog")
    public String setLogger(@RequestParam("param") String jsonStr) {

        // TODO log落盘  g:/opt/module/logs/app.yyyy-MM-dd.log
        log.info(jsonStr);

        // TODO 发送数据到kafka
        kafkaTemplate.send(
            new ProducerRecord<>(KAFKA_ODS_LOG_TOPIC, jsonStr));

        return "success";
    }

}
