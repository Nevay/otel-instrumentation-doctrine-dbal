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
        Util::trace(
            $this->tracer
                ->spanBuilder('START TRANSACTION')
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->startSpan(),
            $this->connection->beginTransaction(...),
        );
    }

    public function commit(): void {
        Util::trace(
            $this->tracer
                ->spanBuilder('COMMIT')
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
                ->startSpan(),
            $this->connection->commit(...),
        );
    }

    public function rollBack(): void {
        Util::trace(
            $this->tracer
                ->spanBuilder('ROLLBACK')
                ->setSpanKind(SpanKind::KIND_CLIENT)
                ->setAttributes($this->connectionAttributes)
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
