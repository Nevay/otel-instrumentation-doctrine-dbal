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
        private readonly DoctrineConfiguration $config,
        private readonly array $connectionAttributes,
    ) {}

    public function prepare(string $sql): Statement {
        $attributes = Util::attributes($sql, !$this->config->captureParameters);

        $statement = Util::trace(
            $this->tracer
                ->spanBuilder(sprintf('PREPARE %s', Util::resolveQuerySpanName($attributes)))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
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
            $this->config,
        );
    }

    public function query(string $sql): Result {
        $attributes = Util::attributes($sql, !$this->config->captureParameters);

        return Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->startSpan(),
            $this->connection->query(...),
            $sql,
        );
    }

    public function quote(string $value): string {
        return $this->connection->quote($value);
    }

    public function exec(string $sql): int|string {
        $attributes = Util::attributes($sql, !$this->config->captureParameters);

        return Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->startSpan(),
            $this->connection->exec(...),
            $sql,
        );
    }

    public function lastInsertId(): int|string {
        static $attributes = [];

        return Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->startSpan(),
            $this->connection->lastInsertId(...),
        );
    }

    public function beginTransaction(): void {
        static $attributes = [
            'db.operation.name' => 'START TRANSACTION',
        ];

        Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->startSpan(),
            $this->connection->beginTransaction(...),
        );
    }

    public function commit(): void {
        static $attributes = [
            'db.operation.name' => 'COMMIT',
        ];

        Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
                ->startSpan(),
            $this->connection->commit(...),
        );
    }

    public function rollBack(): void {
        static $attributes = [
            'db.operation.name' => 'ROLLBACK',
        ];

        Util::trace(
            $this->tracer
                ->spanBuilder(Util::resolveQuerySpanName($attributes))
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->setAttributes($attributes)
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
