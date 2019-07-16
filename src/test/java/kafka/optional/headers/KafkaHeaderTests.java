package kafka.optional.headers;

import io.micronaut.configuration.kafka.annotation.*;
import io.micronaut.configuration.kafka.config.AbstractKafkaConfiguration;
import io.micronaut.context.annotation.Property;
import io.micronaut.messaging.annotation.Header;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.validation.constraints.NotNull;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
@Property(name = AbstractKafkaConfiguration.EMBEDDED_TOPICS, value = "test")
@Property(name = AbstractKafkaConfiguration.EMBEDDED, value = "true")
public class KafkaHeaderTests {

    private static final String TOPIC = "test";
    private static CountDownLatch latch1 = new CountDownLatch(1);
    private static CountDownLatch latch2 = new CountDownLatch(1);

    @Inject
    TestClient kafkaClient;

    @Test
    public void sendMessageWithOptionalHeader() throws Exception {

        kafkaClient.send("1", "test1", "green", "Hello World");

        assertTrue(latch1.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void sendMessageWithoutOptionalHeader() throws Exception {

        kafkaClient.send("2", "test2", null, "Hello World");

        assertTrue(latch2.await(10, TimeUnit.SECONDS));
    }

    @KafkaClient
    @Topic(TOPIC)
    interface TestClient {
        void send(@NotNull @KafkaKey String key,
                  @NotNull @Header String type,
                  @Nullable @Header String color,
                  @NotNull String payload);
    }

    @KafkaListener(offsetReset = OffsetReset.EARLIEST)
    @Topic(TOPIC)
    public static class TestListener {
        void receive(@KafkaKey String key, @Header String type, @Header Optional<String> color, String payload) {
            if (color.isPresent()) {
                latch1.countDown();
            } else {
                latch2.countDown();
            }
        }
    }
}
