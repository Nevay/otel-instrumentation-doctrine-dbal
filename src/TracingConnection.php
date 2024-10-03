<?php

namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\TracerInterface;
use function sprintf;

final class TracingConnection implements Connection {

    public function __construct(
        private readonly Connection $connection,
        private readonly TracerInterface $tracer,
        private readonly array $connectionAttributes,
    ) {}

    public function prepare(string $sql): Statement {
        $attributes = Util::attributes($sql);
        $prepareAttributes = Util::prefixOperationName($attributes, 'PREPARE');

        $statement = Util::trace(
            $this->tracer
                ->spanBuilder(self::resolveSpanName($prepareAttributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($prepareAttributes)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->prepare(...),
            $sql,
        );

        return new TracingStatement(
            $statement,
            $this->tracer
                ->spanBuilder(self::resolveSpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes),
        );
    }

    public function query(string $sql): Result {
        return Util::trace(
            $this->querySpanBuilder($sql)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->query(...),
            $sql,
        );
    }

    public function quote(string $value): string {
        return $this->connection->quote($value);
    }

    public function exec(string $sql): int|string {
        return Util::trace(
            $this->querySpanBuilder($sql)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->exec(...),
            $sql,
        );
    }

    public function lastInsertId(): int|string {
        return Util::trace(
            $this->nonQuerySpanBuilder('LAST_INSERT_ID')
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->lastInsertId(...),
        );
    }

    public function beginTransaction(): void {
        Util::trace(
            $this->nonQuerySpanBuilder('START TRANSACTION')
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->beginTransaction(...),
        );
    }

    public function commit(): void {
        Util::trace(
            $this->nonQuerySpanBuilder('COMMIT')
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->commit(...),
        );
    }

    public function rollBack(): void {
        Util::trace(
            $this->nonQuerySpanBuilder('ROLLBACK')
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->rollBack(...),
        );
    }

    public function getNativeConnection() {
        return $this->connection->getNativeConnection();
    }

    public function getServerVersion(): string {
        return $this->connection->getServerVersion();
    }

    private function resolveSpanName(array $attributes): string {
        $name = $attributes['db.operation.name'] ?? 'SQL';
        if (isset($attributes['db.collection.name'])) {
            $name .= ' ';
            $name .= $attributes['db.collection.name'];
        }

        return $name;
    }

    private function querySpanBuilder(string $sql): SpanBuilderInterface {
        $attributes = Util::attributes($sql);

        return $this->tracer
            ->spanBuilder(self::resolveSpanName($attributes))
            ->setSpanKind(SpanKind::KIND_CLIENT)
            ->setAttributes($this->connectionAttributes)
            ->setAttributes($attributes);
    }

    private function nonQuerySpanBuilder(string $prefix): SpanBuilderInterface {
        return $this->tracer
            ->spanBuilder(sprintf('%s %s', $prefix, $this->connectionAttributes['db.system']))
            ->setSpanKind(SpanKind::KIND_CLIENT)
            ->setAttributes($this->connectionAttributes);
    }
}
