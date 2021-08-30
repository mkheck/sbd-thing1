package com.thehecklers.thing1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

import java.time.Instant;

@SpringBootApplication
public class Thing1Application {

    public static void main(String[] args) {
        Hooks.onErrorDropped(e -> System.out.println("Client disconnected, good bye!"));
        SpringApplication.run(Thing1Application.class, args);
    }

    @Bean
    RSocketRequester requester(RSocketRequester.Builder builder) {
        return builder.tcp("localhost", 7635);
    }
}

@Controller
@AllArgsConstructor
class Thing1Controller {
    private final RSocketRequester requester;

    @MessageMapping("reqstream")
    Flux<Aircraft> reqStream(Mono<Instant> tsMono) {
        return tsMono.doOnNext(ts -> System.out.println("⏰ " + ts))
                .thenMany(requester.route("acstream")
                        .data(Instant.now())
                        .retrieveFlux(Aircraft.class));
    }

    @MessageMapping("channel")
    Flux<Aircraft> channel(Flux<Weather> weatherFlux) {
        return weatherFlux.doOnSubscribe(sub -> System.out.println("SUBSCRIBED TO WEATHER!"))
                .doOnNext(wx -> System.out.println("☀️ " + wx))
                .switchMap(wx -> requester.route("acstream")
                        .data(Instant.now())
                        .retrieveFlux(Aircraft.class));
    }
}

@Data
@AllArgsConstructor
class Weather {
    private Instant when;
    private String observation;
}

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
class Aircraft {
    private String callsign, reg, flightno, type;
    private int altitude, heading, speed;
    private double lat, lon;
}