<?php declare(strict_types=1);
namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Amp\ByteStream\WritableBuffer;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\DriverManager;
use Nevay\OTelSDK\Common\TestClock;
use Nevay\OTelSDK\Otlp\OtlpStreamSpanExporter;
use Nevay\OTelSDK\Trace\IdGenerator\RandomIdGenerator;
use Nevay\OTelSDK\Trace\SpanProcessor\BatchSpanProcessor;
use Nevay\OTelSDK\Trace\TracerConfig;
use Nevay\OTelSDK\Trace\TracerProviderBuilder;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanContext;
use OpenTelemetry\Context\Context;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\Depends;
use PHPUnit\Framework\TestCase;
use Random\Engine\Mt19937;
use Random\Randomizer;
use function json_decode;
use function json_encode;

#[CoversClass(TracingMiddleware::class)]
#[CoversClass(TracingDriver::class)]
#[CoversClass(TracingConnection::class)]
#[CoversClass(TracingStatement::class)]
#[CoversClass(Util::class)]
final class TracingTest extends TestCase {

    public function setUp(): void {
        Context::storage()->attach(Span::wrap(SpanContext::create('0af7651916cd43dd8448eb211c80319c', 'b7ad6b7169203331', 0x3))->storeInContext(Context::getCurrent()));
    }

    public function tearDown(): void {
        Context::storage()->scope()?->detach();
    }

    public function testConnect(): void {
        $tracerProvider = (new TracerProviderBuilder())
            ->setClock(new TestClock())
            ->setIdGenerator(new RandomIdGenerator(new Randomizer(new Mt19937(0))))
            ->addSpanProcessor(new BatchSpanProcessor(new OtlpStreamSpanExporter($buffer = new WritableBuffer())))
            ->build();
        $connection = DriverManager::getConnection(
            ['driver' => 'sqlite3', 'memory' => true],
            (new Configuration())->setMiddlewares([new TracingMiddleware($tracerProvider)]),
        );

        $connection->getNativeConnection();

        $tracerProvider->shutdown();
        $buffer->close();

        $this->assertJsonStringEqualsJsonString(
            <<<JSON
            [
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "ac0a7f8c2faac497",
                "flags": 259,
                "name": "CONNECT",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "code.function", "value": { "stringValue": "connect" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Driver" }}
                ],
                "status":{}
              }
            ]
            JSON,
            json_encode(json_decode($buffer->buffer())->resourceSpans[0]->scopeSpans[0]->spans ?? []),
        );
    }

    #[Depends('testConnect')]
    public function testPrepare(): void {
        $tracerProvider = (new TracerProviderBuilder())
            ->setClock(new TestClock())
            ->setIdGenerator(new RandomIdGenerator(new Randomizer(new Mt19937(0))))
            ->addSpanProcessor(new BatchSpanProcessor(new OtlpStreamSpanExporter($buffer = new WritableBuffer())))
            ->build();
        $connection = DriverManager::getConnection(
            ['driver' => 'sqlite3', 'memory' => true],
            (new Configuration())->setMiddlewares([new TracingMiddleware($tracerProvider)]),
        );

        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = true);
        $connection->executeStatement(<<<SQL
            create table user (
                id int primary key,
                first_name varchar not null,
                last_name varchar not null
            );
            insert into user values (1, 'John', 'Doe');
            insert into user values (2, 'Jane', 'Doe');
            SQL);
        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = false);

        $statement = $connection->prepare('select * from user where first_name = ? and last_name = ?');
        $statement->bindValue(0, 'Jane');
        $statement->bindValue(1, 'Doe');
        $statement->executeQuery();

        $tracerProvider->shutdown();
        $buffer->close();

        $this->assertJsonStringEqualsJsonString(
            <<<JSON
            [
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "ac0a7f8c2faac497",
                "flags": 259,
                "name": "PREPARE SELECT user",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "db.collection.name", "value": { "stringValue": "user" }},
                  { "key": "db.operation.name", "value": { "stringValue": "PREPARE SELECT" }},
                  { "key": "db.query.text", "value": { "stringValue": "select * from user where first_name = ? and last_name = ?" }},
                  { "key": "code.function", "value": { "stringValue": "prepare" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Connection" }}
                ],
                "status":{}
              },
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "75a616b7c0cc21d8",
                "flags": 259,
                "name": "SELECT user",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "db.collection.name", "value": { "stringValue": "user" }},
                  { "key": "db.operation.name", "value": { "stringValue": "SELECT" }},
                  { "key": "db.query.text", "value": { "stringValue": "select * from user where first_name = ? and last_name = ?" }},
                  { "key": "code.function", "value": { "stringValue": "execute" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Statement" }}
                ],
                "status":{}
              }
            ]
            JSON,
            json_encode(json_decode($buffer->buffer())->resourceSpans[0]->scopeSpans[0]->spans ?? []),
        );
    }

    #[Depends('testConnect')]
    public function testSanitize(): void {
        $tracerProvider = (new TracerProviderBuilder())
            ->setClock(new TestClock())
            ->setIdGenerator(new RandomIdGenerator(new Randomizer(new Mt19937(0))))
            ->addSpanProcessor(new BatchSpanProcessor(new OtlpStreamSpanExporter($buffer = new WritableBuffer())))
            ->build();
        $connection = DriverManager::getConnection(
            ['driver' => 'sqlite3', 'memory' => true],
            (new Configuration())->setMiddlewares([new TracingMiddleware($tracerProvider)]),
        );

        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = true);
        $connection->executeStatement(<<<SQL
            create table user (
                id int primary key,
                first_name varchar not null,
                last_name varchar not null
            );
            insert into user values (1, 'John', 'Doe');
            insert into user values (2, 'Jane', 'Doe');
            SQL);
        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = false);

        $connection->executeQuery("select * from user where first_name = 'Jane' and last_name = 'Doe'");

        $tracerProvider->shutdown();
        $buffer->close();

        $this->assertJsonStringEqualsJsonString(
            <<<JSON
            [
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "ac0a7f8c2faac497",
                "flags": 259,
                "name": "SELECT user",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "db.collection.name", "value": { "stringValue": "user" }},
                  { "key": "db.operation.name", "value": { "stringValue": "SELECT" }},
                  { "key": "db.query.text", "value": { "stringValue": "select * from user where first_name = ? and last_name = ?" }},
                  { "key": "code.function", "value": { "stringValue": "query" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Connection" }}
                ],
                "status":{}
              }
            ]
            JSON,
            json_encode(json_decode($buffer->buffer())->resourceSpans[0]->scopeSpans[0]->spans ?? []),
        );
    }

    #[Depends('testConnect')]
    public function testBatch(): void {
        $tracerProvider = (new TracerProviderBuilder())
            ->setClock(new TestClock())
            ->setIdGenerator(new RandomIdGenerator(new Randomizer(new Mt19937(0))))
            ->addSpanProcessor(new BatchSpanProcessor(new OtlpStreamSpanExporter($buffer = new WritableBuffer())))
            ->build();
        $connection = DriverManager::getConnection(
            ['driver' => 'sqlite3', 'memory' => true],
            (new Configuration())->setMiddlewares([new TracingMiddleware($tracerProvider)]),
        );

        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = true);
        $connection->executeStatement(<<<SQL
            create table user (
                id int primary key,
                first_name varchar not null,
                last_name varchar not null
            );
            SQL);
        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = false);

        $connection->executeQuery(<<<SQL
            insert into user values (1, 'John', 'Doe');
            insert into user values (2, 'Jane', 'Doe');
            SQL);

        $tracerProvider->shutdown();
        $buffer->close();

        $this->assertJsonStringEqualsJsonString(
            <<<JSON
            [
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "ac0a7f8c2faac497",
                "flags": 259,
                "name": "BATCH INSERT user",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "db.collection.name", "value": { "stringValue": "user" }},
                  { "key": "db.operation.name", "value": { "stringValue": "BATCH INSERT" }},
                  { "key": "db.operation.batch.size", "value": { "intValue": "2" }},
                  { "key": "db.query.text", "value": { "stringValue": "insert into user values (?, ?, ?);\\ninsert into user values (?, ?, ?);" }},
                  { "key": "code.function", "value": { "stringValue": "query" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Connection" }}
                ],
                "status":{}
              }
            ]
            JSON,
            json_encode(json_decode($buffer->buffer())->resourceSpans[0]->scopeSpans[0]->spans ?? []),
        );
    }

    #[Depends('testConnect')]
    #[Depends('testSanitize')]
    public function testTransactionCommit(): void {
        $tracerProvider = (new TracerProviderBuilder())
            ->setClock(new TestClock())
            ->setIdGenerator(new RandomIdGenerator(new Randomizer(new Mt19937(0))))
            ->addSpanProcessor(new BatchSpanProcessor(new OtlpStreamSpanExporter($buffer = new WritableBuffer())))
            ->build();
        $connection = DriverManager::getConnection(
            ['driver' => 'sqlite3', 'memory' => true],
            (new Configuration())->setMiddlewares([new TracingMiddleware($tracerProvider)]),
        );

        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = true);
        $connection->executeStatement(<<<SQL
            create table user (
                id int primary key,
                first_name varchar not null,
                last_name varchar not null
            );
            insert into user values (1, 'John', 'Doe');
            insert into user values (2, 'Jane', 'Doe');
            SQL);
        $tracerProvider->updateConfigurator(static fn(TracerConfig $config) => $config->disabled = false);

        $connection->beginTransaction();
        $connection->executeQuery("select * from user where first_name = 'Jane' and last_name = 'Doe'");
        $connection->commit();

        $tracerProvider->shutdown();
        $buffer->close();

        $this->assertJsonStringEqualsJsonString(
            <<<JSON
            [
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "ac0a7f8c2faac497",
                "flags": 259,
                "name": "START TRANSACTION",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "code.function", "value": { "stringValue": "beginTransaction" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Connection" }}
                ],
                "status":{}
              },
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "75a616b7c0cc21d8",
                "flags": 259,
                "name": "SELECT user",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "db.collection.name", "value": { "stringValue": "user" }},
                  { "key": "db.operation.name", "value": { "stringValue": "SELECT" }},
                  { "key": "db.query.text", "value": { "stringValue": "select * from user where first_name = ? and last_name = ?" }},
                  { "key": "code.function", "value": { "stringValue": "query" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Connection" }}
                ],
                "status":{}
              },
              {
                "traceId": "0af7651916cd43dd8448eb211c80319c",
                "parentSpanId": "b7ad6b7169203331",
                "spanId": "43b34e9afb52a2db",
                "flags": 259,
                "name": "COMMIT",
                "kind": 3,
                "attributes": [
                  { "key": "db.system", "value": { "stringValue": "sqlite" }},
                  { "key": "code.function", "value": { "stringValue": "commit" }},
                  { "key": "code.namespace", "value": { "stringValue": "Doctrine\\\\DBAL\\\\Driver\\\\SQLite3\\\\Connection" }}
                ],
                "status":{}
              }
            ]
            JSON,
            json_encode(json_decode($buffer->buffer())->resourceSpans[0]->scopeSpans[0]->spans ?? []),
        );
    }
}
