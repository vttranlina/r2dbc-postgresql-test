/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/

import static java.util.Collections.singletonMap;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class R2dbcPostgresqlTest {
    private static final Logger logger = LoggerFactory.getLogger(R2dbcPostgresqlTest.class);
    static String pgUsername = "james";
    static String pgPassword = "secret1";
    static String pgDb = "james";
    static String pgSchema = "public";

    static PostgreSQLContainer<?> PG_CONTAINER = new PostgreSQLContainer<>("postgres:16.1")
        .withDatabaseName(pgDb)
        .withUsername(pgUsername)
        .withPassword(pgPassword)
        .withCreateContainerCmdModifier(cmd -> cmd.withName("r2dbc-postgresql-test-" + UUID.randomUUID()))
        .withTmpFs(singletonMap("/var/lib/postgresql/data", "rw"));

    @BeforeAll
    static void beforeAll() {
        PG_CONTAINER.start();
    }

    @AfterAll
    static void afterAll() {
        PG_CONTAINER.stop();
    }

    @Test
    void shouldNotHanging() throws Exception {
        ConnectionFactory connectionFactory = new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder()
            .host(PG_CONTAINER.getHost())
            .port(PG_CONTAINER.getMappedPort(5432))
            .username(pgUsername)
            .password(pgPassword)
            .database(pgDb)
            .schema(pgSchema)
            .build());

        Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
        Connection connection1 = connectionMono.block();

        // give table message_mailbox
        connectionMono.flatMap(con -> Mono.from(con.createStatement(
                    "CREATE TABLE IF NOT EXISTS message_mailbox (" +
                        "mailbox_id UUID NOT NULL, " +
                        "message_uid BIGINT NOT NULL, " +
                        "message_id UUID NOT NULL, " +
                        "save_date TIMESTAMP(6), " +
                        "PRIMARY KEY (mailbox_id, message_uid)" +
                        ")")
                .execute())
            .flatMap(result -> Mono.from(result.getRowsUpdated()))
            .doFinally(signalType -> con.close())).block();

        // give table message
        connectionMono.flatMap(con -> Mono.from(con.createStatement(
                    "CREATE TABLE message (" +
                        "message_id UUID PRIMARY KEY NOT NULL, " +
                        "body_blob_id VARCHAR(200) NOT NULL, " +
                        "mime_type VARCHAR(200), " +
                        "mime_subtype VARCHAR(200), " +
                        "internal_date TIMESTAMP(6)" +
                        ")")
                .execute())
            .flatMap(result -> Mono.from(result.getRowsUpdated()))
            .doFinally(signalType -> con.close())).block();


        // insert sample data: 100 row
        UUID targetMailboxId = UUID.fromString("018ecb78-7627-7c44-88b5-7f4ec96c757f");

        List<UUID> messageUids = Flux.range(1, 100)
            .map(i -> UUID.randomUUID())
            .collectList()
            .block();

        connectionMono.flatMapMany(connection -> Flux.range(1, 100)
                .flatMap(index -> {
                    UUID messageId = messageUids.get(index - 1);

                    LocalDateTime internalDate = LocalDateTime.now();

                    // Chèn dữ liệu vào bảng message
                    return Mono.from(connection.createStatement(
                            "INSERT INTO message " +
                                "(message_id, body_blob_id, mime_type, mime_subtype, internal_date) " +
                                "VALUES ($1, $2, $3, $4, $5)")
                        .bind("$1", messageId)
                        .bind("$2", "body_blob_" + messageId.toString())
                        .bindNull("$3", String.class) // mime_type
                        .bindNull("$4", String.class) // mime_subtype
                        .bind("$5", internalDate)
                        .execute());
                }))
            .flatMap(Result::getRowsUpdated)
            .collectList().block();

        connectionMono.flatMapMany(connection -> Flux.range(1, 100)
                .flatMap(index -> {
                    UUID mailboxId = (index <= 50) ? targetMailboxId : UUID.randomUUID();
                    long messageUid = index;
                    UUID messageId = messageUids.get(index - 1);
                    LocalDateTime saveDate = LocalDateTime.now();

                    // Chèn dữ liệu vào bảng message
                    return Mono.from(connection.createStatement(
                            "INSERT INTO message_mailbox (mailbox_id, message_uid, message_id, save_date) " +
                                "VALUES ($1, $2, $3, $4)")
                        .bind("$1", mailboxId)
                        .bind("$2", messageUid)
                        .bind("$3", messageId)
                        .bind("$4", saveDate)
                        .execute());
                }))
            .flatMap(Result::getRowsUpdated)
            .collectList().block();

        // When deleted, it's hanging
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger doOnNextCounter = new AtomicInteger(0);
        AtomicInteger fetchMessageCounter = new AtomicInteger(0);

        Flux<String> subscribe = Flux.from(connection1.createStatement("DELETE FROM message_mailbox WHERE mailbox_id = $1 RETURNING message_id")
                .bind(0, targetMailboxId)
                .execute())
            .flatMap(result -> result.map((row, rowMetadata) -> {
                String msgIdWasDeleted = row.get(0, String.class);
                System.out.println("msg was deleted(" + counter.incrementAndGet() + "): " + msgIdWasDeleted);
                return msgIdWasDeleted;
            }))
            .doOnNext(e -> {
                System.out.println("doOnNext(" + doOnNextCounter.incrementAndGet() + "): " + e);
            })
            .map(UUID::fromString)
            .log()
            .concatMap(msgUid -> {
                System.out.println("pre fetch message: " + msgUid);
                return Mono.from(connection1.createStatement("SELECT * FROM message where message_id = $1")
                        .bind(0, msgUid)
                        .execute())
                    .flatMapMany(result ->
                        result.map((row, rowMetadata) -> {
                            System.out.println("fetch msg completed: " + fetchMessageCounter.incrementAndGet());
                            return row.get("message_id", String.class);
                        }))
                    .last();
            })
            .log();

        subscribe.collectList().block();

        System.out.println("Completed");
    }
}
