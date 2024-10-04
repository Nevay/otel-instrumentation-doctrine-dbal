<?php

namespace Nevay\OTelInstrumentation\DoctrineDbal;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\TracerInterface;

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
                ->spanBuilder(Util::resolveQuerySpanName($prepareAttributes))
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
                ->spanBuilder(Util::resolveQuerySpanName(($attributes)))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes),
        );
    }

    public function query(string $sql): Result {
        $attributes = Util::attributes($sql);

        return Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
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
        $attributes = Util::attributes($sql);

        return Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->exec(...),
            $sql,
        );
    }

    public function lastInsertId(): int|string {
        static $attributes;
        $attributes ??= Util::attributes('SELECT LAST_INSERT_ID()', false);

        return Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->lastInsertId(...),
        );
    }

    public function beginTransaction(): void {
        static $attributes;
        $attributes ??= Util::attributes('BEGIN TRANSACTION', false);

        Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->beginTransaction(...),
        );
    }

    public function commit(): void {
        static $attributes;
        $attributes ??= Util::attributes('COMMIT', false);

        Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->setAttribute('code.function', __FUNCTION__)
                ->setAttribute('code.namespace', $this->connection::class)
                ->startSpan(),
            $this->connection->commit(...),
        );
    }

    public function rollBack(): void {
        static $attributes;
        $attributes ??= Util::attributes('ROLLBACK', false);

        Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
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
}
